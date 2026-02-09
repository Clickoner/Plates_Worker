import { createClient } from "@supabase/supabase-js";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import sharp from "sharp";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const BUCKET = "separations"; // per your note

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing env vars: SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function runCmd(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { ...opts, stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => (out += d.toString()));
    child.stderr.on("data", (d) => (err += d.toString()));
    child.on("close", (code) => {
      if (code === 0) resolve({ out, err });
      else reject(new Error(`${cmd} exited ${code}\n${err || out}`));
    });
  });
}

async function assertGhostscript() {
  const { out } = await runCmd("gs", ["--version"]);
  console.log("✅ Ghostscript found. Version:", out.trim());
}

function safeName(s) {
  return String(s)
    .replace(/[^\w.-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 80);
}

function plateKeyFromLabel(label) {
  const p = label.toLowerCase();
  if (p === "c" || p.includes("cyan")) return "C";
  if (p === "m" || p.includes("magenta")) return "M";
  if (p === "y" || p.includes("yellow")) return "Y";
  if (p === "k" || p.includes("black")) return "K";
  return "SPOT";
}

function spotColorFromName(name) {
  // deterministic “nice” color from plate name
  const h = crypto.createHash("md5").update(name).digest();
  const r = 120 + (h[0] % 120);
  const g = 120 + (h[1] % 120);
  const b = 120 + (h[2] % 120);
  return { r, g, b };
}

/**
 * Convert grayscale plate into a tinted RGB preview:
 * - C: R=gray, G=255, B=255
 * - M: R=255, G=gray, B=255
 * - Y: R=255, G=255, B=gray
 * - K: R=gray, G=gray, B=gray
 * - Spot: tint using deterministic RGB based on name
 */
async function colorizeGrayscale(grayPngBuffer, key, labelForSpot) {
  const { data, info } = await sharp(grayPngBuffer)
    .ensureAlpha()
    .raw()
    .toBuffer({ resolveWithObject: true });

  const out = Buffer.alloc(info.width * info.height * 4);

  let spotRGB = null;
  if (key === "SPOT") spotRGB = spotColorFromName(labelForSpot);

  for (let i = 0; i < info.width * info.height; i++) {
    const gray = data[i * 4]; // grayscale in R channel
    let r, g, b;

    if (key === "C") {
      r = gray; g = 255; b = 255;
    } else if (key === "M") {
      r = 255; g = gray; b = 255;
    } else if (key === "Y") {
      r = 255; g = 255; b = gray;
    } else if (key === "K") {
      r = gray; g = gray; b = gray;
    } else {
      // Spot: stronger color where ink is heavier (lower gray)
      const strength = 1 - gray / 255;
      r = Math.round(255 - (255 - spotRGB.r) * strength);
      g = Math.round(255 - (255 - spotRGB.g) * strength);
      b = Math.round(255 - (255 - spotRGB.b) * strength);
    }

    out[i * 4 + 0] = r;
    out[i * 4 + 1] = g;
    out[i * 4 + 2] = b;
    out[i * 4 + 3] = 255;
  }

  return sharp(out, { raw: { width: info.width, height: info.height, channels: 4 } })
    .png()
    .toBuffer();
}

async function uploadBuffer(storagePath, buffer, contentType = "image/png") {
  const { error } = await supabase.storage.from(BUCKET).upload(storagePath, buffer, {
    contentType,
    upsert: true,
  });
  if (error) throw new Error(`Upload failed (${storagePath}): ${error.message}`);

  const { data } = supabase.storage.from(BUCKET).getPublicUrl(storagePath);
  return data.publicUrl;
}

async function downloadToFile(storagePath, localPath) {
  const { data, error } = await supabase.storage.from(BUCKET).download(storagePath);
  if (error) throw new Error(`Download failed (${storagePath}): ${error.message}`);

  const ab = await data.arrayBuffer();
  await fs.writeFile(localPath, Buffer.from(ab));
}

/**
 * Extract a SINGLE page to a temp PDF so tiffsep runs per-page.
 * pageIndex is 0-based in your payload.
 * Ghostscript uses 1-based page numbers for -dFirstPage/-dLastPage.
 */
async function extractSinglePagePdf(inputPdf, pageIndex, outPdf) {
  const pageNum = Number(pageIndex) + 1;
  await runCmd("gs", [
    "-q",
    "-dNOPAUSE",
    "-dBATCH",
    `-dFirstPage=${pageNum}`,
    `-dLastPage=${pageNum}`,
    "-sDEVICE=pdfwrite",
    "-o",
    outPdf,
    inputPdf,
  ]);
}

/**
 * Run Ghostscript tiffsep against the single-page PDF.
 * Returns list of TIFFs produced.
 */
async function gsExtractTiffSeps(singlePagePdf, outDir, dpi) {
  const outPattern = path.join(outDir, "sep_%03d.tif");

  await runCmd("gs", [
    "-q",
    "-dNOPAUSE",
    "-dBATCH",
    "-sDEVICE=tiffsep",
    `-r${dpi}`,
    "-o",
    outPattern,
    singlePagePdf,
  ]);

  const files = (await fs.readdir(outDir))
    .filter((f) => f.toLowerCase().endsWith(".tif") || f.toLowerCase().endsWith(".tiff"))
    .map((f) => path.join(outDir, f));

  if (!files.length) throw new Error("Ghostscript produced no TIFF separation files.");
  return files;
}

async function tiffToGrayPng(tiffPath) {
  return sharp(tiffPath).grayscale().png().toBuffer();
}

/**
 * We try to infer plate names from filename.
 * If Ghostscript produces generic names (sep_001.tif),
 * we still handle it, but you’ll get "sep_001" etc as spot names.
 * If you want “real” spot names (Pantone/Varnish/etc),
 * we’ll need a later enhancement to parse plate names from the PDF.
 */
function guessPlateLabelFromFilename(filePath) {
  const base = path.basename(filePath).toLowerCase();

  if (base.includes("cyan")) return "Cyan";
  if (base.includes("magenta")) return "Magenta";
  if (base.includes("yellow")) return "Yellow";
  if (base.includes("black")) return "Black";

  return safeName(path.basename(filePath, path.extname(filePath)));
}

async function setProofPageStatus(proofPageId, status, errorMsg = null) {
  const updateObj = {
    separations_status: status,
    separations_error: errorMsg,
  };
  await supabase.from("proof_pages").update(updateObj).eq("id", proofPageId);
}

async function processJob(job) {
  const payload = job.payload || {};

  const proofPageId = payload.proof_page_id;
  const pdfStoragePath = payload.pdf_storage_path;
  const pageIndex = payload.page_index ?? 0;
  const dpi = payload.dpi ?? 150;

  if (!proofPageId || !pdfStoragePath) {
    throw new Error("Job payload missing proof_page_id and/or pdf_storage_path");
  }

  await setProofPageStatus(proofPageId, "processing", null);

  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "plates-"));
  const localPdf = path.join(tmpDir, "input.pdf");
  const singlePagePdf = path.join(tmpDir, "page.pdf");
  const outDir = path.join(tmpDir, "out");
  await fs.mkdir(outDir, { recursive: true });

  console.log("⬇️ Downloading PDF:", pdfStoragePath);
  await downloadToFile(pdfStoragePath, localPdf);

  console.log(`📄 Extracting page ${pageIndex} to single-page PDF...`);
  await extractSinglePagePdf(localPdf, pageIndex, singlePagePdf);

  console.log(`🧪 Running Ghostscript separations @${dpi}dpi...`);
  const tiffFiles = await gsExtractTiffSeps(singlePagePdf, outDir, dpi);

  // Upload outputs to: separations/<proof_page_id>/...
  const plateFolder = `separations/${proofPageId}`;

  const results = {
    proof_page_id: proofPageId,
    page_index: pageIndex,
    dpi,
    bucket: BUCKET,
    plates: {},
    spots: [],
  };

  let cUrl = null, mUrl = null, yUrl = null, kUrl = null;
  const spotPlatesArr = [];

  for (const tiff of tiffFiles) {
    const plateLabel = guessPlateLabelFromFilename(tiff);
    const key = plateKeyFromLabel(plateLabel);

    const grayPng = await tiffToGrayPng(tiff);
    const tinted = await colorizeGrayscale(grayPng, key, plateLabel);

    const grayPath = `${plateFolder}/${safeName(plateLabel)}_gray.png`;
    const tintPath = `${plateFolder}/${safeName(plateLabel)}.png`;

    const grayUrl = await uploadBuffer(grayPath, grayPng, "image/png");
    const tintUrl = await uploadBuffer(tintPath, tinted, "image/png");

    results.plates[plateLabel] = { gray: grayUrl, tinted: tintUrl };

    if (key === "C") cUrl = tintUrl;
    else if (key === "M") mUrl = tintUrl;
    else if (key === "Y") yUrl = tintUrl;
    else if (key === "K") kUrl = tintUrl;
    else {
      results.spots.push({ name: plateLabel, url: tintUrl, gray_url: grayUrl });
      spotPlatesArr.push({ name: plateLabel, url: tintUrl });
    }
  }

  // Update proof_pages with URLs + spot_plates inline JSONB
  const updateObj = {
    c_url: cUrl,
    m_url: mUrl,
    y_url: yUrl,
    k_url: kUrl,
    spot_plates: spotPlatesArr,
    separations_status: "done",
    separations_error: null,
  };

  // Only set keys that exist (avoid overwriting with null if a plate isn't produced)
  Object.keys(updateObj).forEach((k) => {
    if (updateObj[k] === null) delete updateObj[k];
  });

  const { error: ppErr } = await supabase.from("proof_pages").update(updateObj).eq("id", proofPageId);
  if (ppErr) throw new Error(`Updating proof_pages failed: ${ppErr.message}`);

  // Cleanup
  try { await fs.rm(tmpDir, { recursive: true, force: true }); } catch {}

  return results;
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
  if (!jobs || jobs.length === 0) return;

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

  console.log("✅ Picked up job:", job.id);

  try {
    const result = await processJob(job);

    const { error: doneErr } = await supabase
      .from("plate_jobs")
      .update({ status: "done", result, error: null })
      .eq("id", job.id);

    if (doneErr) throw new Error(doneErr.message);

    console.log("🎉 Job done:", job.id);
  } catch (e) {
    const msg = e?.message || String(e);
    console.error("❌ Job failed:", job.id, msg);

    // best-effort: set proof_pages error if we can read proof_page_id
    const proofPageId = job?.payload?.proof_page_id;
    if (proofPageId) {
      try {
        await setProofPageStatus(proofPageId, "failed", msg);
      } catch {}
    }

    await supabase
      .from("plate_jobs")
      .update({ status: "failed", error: msg })
      .eq("id", job.id);
  }
}

async function loop() {
  await assertGhostscript();
  console.log("Plates worker started ✅");

  while (true) {
    await tryClaimOneJob();
    await sleep(2000);
  }
}

loop().catch((e) => {
  console.error("Fatal worker error:", e?.message || e);
  process.exit(1);
});
