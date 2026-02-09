import fs from "fs";
import os from "os";
import path from "path";
import { execFile } from "child_process";
import { promisify } from "util";
import sharp from "sharp";
import { createClient } from "@supabase/supabase-js";

const execFileAsync = promisify(execFile);

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing env vars. Need SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const BUCKET = "separations";
const POLL_MS = 1500;

// ---- helpers
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function safeBasename(name) {
  return (name || "spot")
    .replace(/[^\w\-\. ]+/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80);
}

async function uploadAndGetPublicUrl(storagePath, fileBuffer, contentType = "image/png") {
  // Upsert so re-runs overwrite
  const { error: upErr } = await supabase.storage.from(BUCKET).upload(storagePath, fileBuffer, {
    upsert: true,
    contentType,
    cacheControl: "3600",
  });
  if (upErr) throw new Error(`Upload failed (${storagePath}): ${upErr.message}`);

  const { data } = supabase.storage.from(BUCKET).getPublicUrl(storagePath);
  if (!data?.publicUrl) throw new Error(`No public URL returned for ${storagePath}`);
  return data.publicUrl;
}

/**
 * Convert a grayscale separation TIFF into a colored RGBA PNG where:
 * - RGB is fixed to the plate color
 * - Alpha is ink coverage (derived from grayscale)
 *
 * Assumption: TIFF grayscale is "paper=white(255), ink=black(0)" -> alpha = 255 - gray
 */
async function makePlatePngFromTiff(tiffPath, rgb /* [r,g,b] */) {
  const { data, info } = await sharp(tiffPath)
    .ensureAlpha()
    .raw()
    .toBuffer({ resolveWithObject: true });

  // raw is RGBA even for grayscale conversions; we use R as gray
  const out = Buffer.alloc(info.width * info.height * 4);

  for (let i = 0; i < info.width * info.height; i++) {
    const gray = data[i * 4]; // 0..255
    const alpha = 255 - gray; // ink amount

    out[i * 4 + 0] = rgb[0];
    out[i * 4 + 1] = rgb[1];
    out[i * 4 + 2] = rgb[2];
    out[i * 4 + 3] = alpha;
  }

  return sharp(out, {
    raw: { width: info.width, height: info.height, channels: 4 },
  })
    .png()
    .toBuffer();
}

/**
 * Build a composite preview PNG by multiplying plate layers onto white.
 * This is a simple visual composite; your viewer may also do blending client-side.
 */
async function makeCompositePng(platePngs /* array of {buffer} */) {
  if (!platePngs.length) return null;

  // Use first plate to get dimensions
  const meta = await sharp(platePngs[0].buffer).metadata();
  const width = meta.width || 1;
  const height = meta.height || 1;

  // Start with white background
  let base = sharp({
    create: { width, height, channels: 4, background: { r: 255, g: 255, b: 255, alpha: 1 } },
  });

  // Multiply each plate on top
  // (sharp composite "multiply" uses RGB; alpha is respected)
  const composites = platePngs.map((p) => ({
    input: p.buffer,
    blend: "multiply",
  }));

  return base.composite(composites).png().toBuffer();
}

function classifySeparationFile(fileName) {
  const lower = fileName.toLowerCase();

  // Ghostscript tiffsep often outputs names containing "cyan", "magenta", etc.
  if (lower.includes("cyan")) return { kind: "cmyk", key: "c" };
  if (lower.includes("magenta")) return { kind: "cmyk", key: "m" };
  if (lower.includes("yellow")) return { kind: "cmyk", key: "y" };
  if (lower.includes("black")) return { kind: "cmyk", key: "k" };

  // Otherwise treat as spot (Pantone/varnish/white/dieline etc)
  return { kind: "spot", key: null };
}

async function downloadPdfToTemp(pdfStoragePath) {
  const { data, error } = await supabase.storage.from(BUCKET).download(pdfStoragePath);
  if (error) throw new Error(`PDF download failed (${pdfStoragePath}): ${error.message}`);
  const arrayBuf = await data.arrayBuffer();
  return Buffer.from(arrayBuf);
}

async function runGhostscriptTiffSep({ pdfPath, outDir, pageIndex, dpi }) {
  // gs uses 1-based page numbers
  const pageNum = (pageIndex ?? 0) + 1;

  // Output pattern; gs will create multiple files for each separation
  const outPattern = path.join(outDir, "sep-%04d.tif");

  const args = [
    "-dSAFER",
    "-dBATCH",
    "-dNOPAUSE",
    `-r${dpi || 150}`,
    "-sDEVICE=tiffsep",
    `-dFirstPage=${pageNum}`,
    `-dLastPage=${pageNum}`,
    `-sOutputFile=${outPattern}`,
    pdfPath,
  ];

  await execFileAsync("gs", args, { timeout: 10 * 60 * 1000 }); // 10 min
}

async function processOneJob(job) {
  const payload = job.payload || {};
  const pdfStoragePath = payload.pdf_storage_path; // IMPORTANT: matches your actual payload
  const proofPageId = payload.proof_page_id;
  const pageIndex = payload.page_index ?? 0;
  const dpi = payload.dpi ?? 150;

  if (!pdfStoragePath || !proofPageId) {
    throw new Error("Job payload missing pdf_storage_path or proof_page_id");
  }

  // Set proof_pages status early so UI can show progress
  await supabase
    .from("proof_pages")
    .update({ separations_status: "processing", separations_error: null })
    .eq("id", proofPageId);

  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plates-"));
  const pdfPath = path.join(tmpRoot, "input.pdf");
  const sepDir = path.join(tmpRoot, "out");
  fs.mkdirSync(sepDir, { recursive: true });

  try {
    // 1) Download PDF
    const pdfBuf = await downloadPdfToTemp(pdfStoragePath);
    fs.writeFileSync(pdfPath, pdfBuf);

    // 2) Run Ghostscript separation
    await runGhostscriptTiffSep({ pdfPath, outDir: sepDir, pageIndex, dpi });

    // 3) Collect TIFF outputs
    const files = fs.readdirSync(sepDir).filter((f) => f.toLowerCase().endsWith(".tif"));
    if (!files.length) throw new Error("Ghostscript produced no separation TIFFs");

    // 4) Convert to colored PNG plates + upload
    const plateColor = {
      c: [0, 255, 255],
      m: [255, 0, 255],
      y: [255, 255, 0],
      k: [0, 0, 0],
    };

    let c_url = null,
      m_url = null,
      y_url = null,
      k_url = null;

    const spot_plates = [];
    const compositeLayers = [];

    for (const f of files) {
      const full = path.join(sepDir, f);
      const cls = classifySeparationFile(f);

      if (cls.kind === "cmyk") {
        const key = cls.key;
        const pngBuf = await makePlatePngFromTiff(full, plateColor[key]);

        const storagePath = `separations/${proofPageId}/page-${pageIndex}/${key}.png`;
        const url = await uploadAndGetPublicUrl(storagePath, pngBuf, "image/png");

        if (key === "c") c_url = url;
        if (key === "m") m_url = url;
        if (key === "y") y_url = url;
        if (key === "k") k_url = url;

        compositeLayers.push({ buffer: pngBuf });
      } else {
        const spotNameGuess = safeBasename(
          path
            .basename(f, path.extname(f))
            .replace(/^sep-\d+/i, "")
            .replace(/[._-]+/g, " ")
        );

        // spots get a neutral “ink” look; viewer can color it if you want later
        const spotPng = await makePlatePngFromTiff(full, [0, 0, 0]);
        const storagePath = `separations/${proofPageId}/page-${pageIndex}/spot-${spotNameGuess}.png`;
        const url = await uploadAndGetPublicUrl(storagePath, spotPng, "image/png");

        spot_plates.push({ name: spotNameGuess || "Spot", url });
        compositeLayers.push({ buffer: spotPng });
      }
    }

    // 5) Composite (optional but your schema supports it)
    const compositeBuf = await makeCompositePng(compositeLayers);
    let composite_url = null;
    if (compositeBuf) {
      const storagePath = `separations/${proofPageId}/page-${pageIndex}/composite.png`;
      composite_url = await uploadAndGetPublicUrl(storagePath, compositeBuf, "image/png");
    }

    // 6) Update proof_pages with URLs + spot plates
    const update = {
      c_url,
      m_url,
      y_url,
      k_url,
      composite_url,
      spot_plates,
      separations_status: "done",
      separations_error: null,
    };

    const { error: updErr } = await supabase.from("proof_pages").update(update).eq("id", proofPageId);
    if (updErr) throw new Error(`Failed updating proof_pages: ${updErr.message}`);

    return { ok: true, proof_page_id: proofPageId, page_index: pageIndex, ...update };
  } finally {
    // Cleanup temp
    try {
      fs.rmSync(tmpRoot, { recursive: true, force: true });
    } catch {}
  }
}

async function tryClaimOneJob() {
  const { data: jobs, error: findErr } = await supabase
    .from("plate_jobs")
    .select("id,payload,status,locked_at,created_at")
    .eq("status", "queued")
    .is("locked_at", null)
    .order("created_at", { ascending: true })
    .limit(1);

  if (findErr) {
    console.error("Error finding job:", findErr.message);
    return;
  }
  if (!jobs?.length) return;

  const job = jobs[0];

  const { data: locked, error: lockErr } = await supabase
    .from("plate_jobs")
    .update({ status: "processing", locked_at: new Date().toISOString() })
    .eq("id", job.id)
    .eq("status", "queued")
    .is("locked_at", null)
    .select("id")
    .maybeSingle();

  if (lockErr) {
    console.error("Error locking job:", lockErr.message);
    return;
  }
  if (!locked) return;

  console.log("✅ Picked up separation job:", job.id);

  try {
    const result = await processOneJob(job);

    const { error: doneErr } = await supabase
      .from("plate_jobs")
      .update({ status: "done", result })
      .eq("id", job.id);

    if (doneErr) throw new Error(`Error marking job done: ${doneErr.message}`);

    console.log("🎉 Separation job done:", job.id);
  } catch (e) {
    const msg = e?.message || String(e);
    console.error("❌ Separation job failed:", job.id, msg);

    // Best effort: write failure to both tables
    await supabase.from("plate_jobs").update({ status: "failed", error: msg }).eq("id", job.id);

    // If payload had proof_page_id, mark proof_pages error too
    const proofPageId = job?.payload?.proof_page_id;
    if (proofPageId) {
      await supabase
        .from("proof_pages")
        .update({ separations_status: "failed", separations_error: msg })
        .eq("id", proofPageId);
    }
  }
}

async function main() {
  console.log("Plates worker started ✅");
  // Optional: show gs version in logs
  try {
    const { stdout } = await execFileAsync("gs", ["--version"]);
    console.log("✅ Ghostscript found. Version:", String(stdout).trim());
  } catch {
    console.log("⚠️ Ghostscript not found on PATH (gs). Separations will fail.");
  }

  while (true) {
    await tryClaimOneJob();
    await sleep(POLL_MS);
  }
}

main();
