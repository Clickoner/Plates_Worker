import { createClient } from "@supabase/supabase-js";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import { spawn } from "node:child_process";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PLATES_WORKER_SECRET = process.env.PLATES_WORKER_SECRET;

// Buckets (change names if you used different ones)
const PDF_BUCKET = "pdfs";
const PLATES_BUCKET = "plates";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing env vars. Need SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}
if (!PLATES_WORKER_SECRET) {
  console.error("Missing env var. Need PLATES_WORKER_SECRET");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

console.log("Plates worker started ✅");

// -------------------------
// Helpers
// -------------------------
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function safeName(s) {
  return String(s || "")
    .trim()
    .replace(/\s+/g, "_")
    .replace(/[^a-zA-Z0-9._-]/g, "_")
    .slice(0, 80);
}

function runCmd(cmd, args, { cwd } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { cwd, stdio: ["ignore", "pipe", "pipe"] });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.stderr.on("data", (d) => (stderr += d.toString()));

    child.on("error", (err) => reject(err));

    child.on("close", (code) => {
      if (code === 0) return resolve({ stdout, stderr });
      const e = new Error(
        `Command failed (${cmd} ${args.join(" ")}), exit=${code}\n${stderr || stdout}`
      );
      e.stdout = stdout;
      e.stderr = stderr;
      e.code = code;
      reject(e);
    });
  });
}

// -------------------------
// Download PDF
// -------------------------
async function downloadPdf({ pdf_url, storage_path }) {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "plates-"));
  const pdfPath = path.join(tmpDir, "input.pdf");

  if (pdf_url) {
    const res = await fetch(pdf_url);
    if (!res.ok) throw new Error(`Failed to fetch pdf_url: ${res.status} ${res.statusText}`);
    const buf = Buffer.from(await res.arrayBuffer());
    await fs.writeFile(pdfPath, buf);
    return { tmpDir, pdfPath };
  }

  if (storage_path) {
    const { data, error } = await supabase.storage.from(PDF_BUCKET).download(storage_path);
    if (error) throw new Error(`Storage download error: ${error.message}`);
    const buf = Buffer.from(await data.arrayBuffer());
    await fs.writeFile(pdfPath, buf);
    return { tmpDir, pdfPath };
  }

  throw new Error("Payload must include pdf_url OR storage_path");
}

// -------------------------
// List separations (includes spot plates)
// -------------------------
// Uses: gs -o - -sDEVICE=inkcov input.pdf
// Output includes lines like: " 0.12345  0.00000  0.05000  0.00000 CMYK OK"
async function listSeparations(pdfPath) {
  // We’ll still produce a default set (CMYK) even if inkcov is weird.
  const plates = [];

  try {
    const { stdout } = await runCmd("gs", ["-q", "-o", "-", "-sDEVICE=inkcov", pdfPath]);
    // inkcov doesn't explicitly list spot names.
    // So we also try to extract spot plate names via "inkcov" isn't enough.
    // We'll do a second pass using "separation rendering" trick: parse DSC/Resources? Not reliable.
    // Practical approach: render CMYK always, and attempt spot plates via "SeparationColorNames" if present using pdfinfo? (not available).
    // For now: CMYK + "Known special names" that commonly exist as spot inks.
    // (We’ll refine later by parsing actual spot list from PDF via a better tool.)
    plates.push(
      { type: "process", name: "Cyan", key: "C", gsPlane: "Cyan" },
      { type: "process", name: "Magenta", key: "M", gsPlane: "Magenta" },
      { type: "process", name: "Yellow", key: "Y", gsPlane: "Yellow" },
      { type: "process", name: "Black", key: "K", gsPlane: "Black" }
    );

    // Heuristics for common spot plates (we render them if they exist; if not, result will be blank)
    const commonSpots = [
      "Pantone",
      "PANTONE",
      "White",
      "WHITE",
      "Varnish",
      "VARNISH",
      "Spot",
      "SPOT",
      "Die",
      "Dieline",
      "DIE_LINE",
      "Cut",
      "CUT",
    ];

    // We'll include these as "candidate spot plates" (render step will keep only those that produce non-empty output)
    for (const s of commonSpots) {
      plates.push({ type: "spot_candidate", name: s, key: safeName(s), gsPlane: s });
    }

    return plates;
  } catch (e) {
    // If gs isn't installed, this will throw.
    // Still return CMYK so you can see something
    return [
      { type: "process", name: "Cyan", key: "C", gsPlane: "Cyan" },
      { type: "process", name: "Magenta", key: "M", gsPlane: "Magenta" },
      { type: "process", name: "Yellow", key: "Y", gsPlane: "Yellow" },
      { type: "process", name: "Black", key: "K", gsPlane: "Black" },
    ];
  }
}

// -------------------------
// Render one plate to PNGs (per page)
// -------------------------
// We render using Ghostscript "pngalpha" device and try to isolate plates.
// For CMYK, we use -sProcessColorModel=DeviceCMYK and -dUseCIEColor to keep stable output.
// For spot plates, Ghostscript can render separations by setting SeparationColorNames, but it’s finicky.
// We'll do practical v1:
// - Always render CMYK plates.
// - Try render spot candidates using -dSpotColor=true and -sSeparationColorNames (works on many PDFs).
async function renderPlatePNGs({ pdfPath, outDir, plate, dpi = 144 }) {
  const plateKey = safeName(plate.key || plate.name);
  const outPattern = path.join(outDir, `${plateKey}-p%03d.png`);

  // Base args
  const args = [
    "-q",
    "-dSAFER",
    "-dBATCH",
    "-dNOPAUSE",
    `-r${dpi}`,
    "-sDEVICE=pngalpha",
    `-sOutputFile=${outPattern}`,
  ];

  if (plate.type === "process") {
    // Render process plate via Separation (works reliably for CMYK)
    // Ghostscript supports -sSeparationColorNames=...
    // We'll request only this plate.
    args.push("-dProcessColorModel=/DeviceCMYK");
    args.push(`-sSeparationColorNames=${plate.gsPlane}`);
    args.push("-dRenderIntent=3");
  } else {
    // Spot candidate attempt
    // This may produce empty images if that plate doesn't exist (that's fine; we'll filter later)
    args.push("-dProcessColorModel=/DeviceCMYK");
    args.push("-dSpotColor=true");
    args.push(`-sSeparationColorNames=${plate.gsPlane}`);
  }

  args.push(pdfPath);

  await runCmd("gs", args);

  // Return the files created
  const files = (await fs.readdir(outDir))
    .filter((f) => f.startsWith(`${plateKey}-p`) && f.endsWith(".png"))
    .map((f) => path.join(outDir, f))
    .sort();

  return files;
}

// Check if a PNG is "basically empty" (very rough heuristic by file size)
async function isProbablyEmptyPng(filePath) {
  const st = await fs.stat(filePath);
  // A rendered blank page often is tiny; tune later.
  return st.size < 4000;
}

// -------------------------
// Upload results to Supabase Storage
// -------------------------
async function uploadFileToStorage({ localPath, storageKey, contentType = "image/png" }) {
  const buf = await fs.readFile(localPath);

  const { error } = await supabase.storage.from(PLATES_BUCKET).upload(storageKey, buf, {
    contentType,
    upsert: true,
  });

  if (error) throw new Error(`Upload failed (${storageKey}): ${error.message}`);

  // Return a public URL-like path (you can also use signed URLs if bucket is private)
  const { data } = supabase.storage.from(PLATES_BUCKET).getPublicUrl(storageKey);
  return data.publicUrl;
}

// -------------------------
// Main "do work" function
// -------------------------
async function processJob(job) {
  // security check
  if (job.payload?.secret !== PLATES_WORKER_SECRET) {
    throw new Error("Invalid payload.secret (PLATES_WORKER_SECRET mismatch)");
  }

  const { tmpDir, pdfPath } = await downloadPdf({
    pdf_url: job.payload?.pdf_url,
    storage_path: job.payload?.storage_path,
  });

  const outDir = path.join(tmpDir, "out");
  await fs.mkdir(outDir, { recursive: true });

  // jobRunId used in filenames
  const jobRunId = crypto.randomBytes(8).toString("hex");
  const baseKey = `jobs/${job.id}/${jobRunId}`;

  // 1) detect/list plates (v1 includes CMYK + spot candidates)
  const plateList = await listSeparations(pdfPath);

  // 2) render plates
  const rendered = [];
  for (const plate of plateList) {
    try {
      const files = await renderPlatePNGs({ pdfPath, outDir, plate, dpi: job.payload?.dpi || 144 });

      if (!files.length) continue;

      // Filter out mostly-empty spot candidates
      if (plate.type !== "process") {
        let allEmpty = true;
        for (const f of files) {
          if (!(await isProbablyEmptyPng(f))) {
            allEmpty = false;
            break;
          }
        }
        if (allEmpty) continue;
      }

      // Upload each page image
      const pages = [];
      for (const f of files) {
        const filename = path.basename(f);
        const storageKey = `${baseKey}/${safeName(plate.key || plate.name)}/${filename}`;
        const url = await uploadFileToStorage({ localPath: f, storageKey });
        pages.push({ filename, url });
      }

      rendered.push({
        name: plate.name,
        key: safeName(plate.key || plate.name),
        type: plate.type === "process" ? "process" : "spot",
        pages,
      });
    } catch (e) {
      // If a single plate fails, continue others
      console.error(`Plate render failed: ${plate.name}`, e?.message || e);
    }
  }

  // 3) Build result for viewer
  const result = {
    ok: true,
    processed_at: new Date().toISOString(),
    job_id: job.id,
    input: {
      pdf_url: job.payload?.pdf_url || null,
      storage_path: job.payload?.storage_path || null,
    },
    plates: rendered,
  };

  // cleanup best-effort
  await fs.rm(tmpDir, { recursive: true, force: true });

  return result;
}

// -------------------------
// Worker loop (claim jobs)
// -------------------------
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

  // lock it
  const { data: locked, error: lockErr } = await supabase
    .from("plate_jobs")
    .update({ status: "processing", locked_at: new Date().toISOString() })
    .eq("id", job.id)
    .eq("status", "queued")
    .is("locked_at", null)
    .select("id,payload")
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

    if (doneErr) throw new Error(`Error marking done: ${doneErr.message}`);

    console.log("🎉 Job done:", job.id);
  } catch (e) {
    const msg = e?.message || String(e);
    console.error("Job failed:", job.id, msg);

    await supabase
      .from("plate_jobs")
      .update({ status: "failed", error: msg })
      .eq("id", job.id);
  }
}

async function loop() {
  while (true) {
    try {
      await tryClaimOneJob();
    } catch (e) {
      console.error("Worker loop error:", e?.message || e);
    }
    await sleep(2000);
  }
}

loop();
