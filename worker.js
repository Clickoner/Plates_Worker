import { createClient } from "@supabase/supabase-js";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";

const execFileAsync = promisify(execFile);

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing env vars. Need SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

console.log("Plates worker started ✅");

async function ensureTools() {
  try {
    const { stdout } = await execFileAsync("gs", ["--version"]);
    console.log("✅ Ghostscript found. Version:", stdout.trim());
  } catch (e) {
    console.error("❌ Ghostscript (gs) not found on PATH. Your Dockerfile must install ghostscript.");
    throw e;
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function downloadFromBucket(bucket, filePath, outFile) {
  const { data, error } = await supabase.storage.from(bucket).download(filePath);
  if (error) throw new Error(`Download failed: ${error.message}`);

  const arrayBuffer = await data.arrayBuffer();
  await fsp.writeFile(outFile, Buffer.from(arrayBuffer));
}

async function uploadToBucket(bucket, filePath, contentType, buffer) {
  const { error } = await supabase.storage.from(bucket).upload(filePath, buffer, {
    contentType,
    upsert: true,
  });
  if (error) throw new Error(`Upload failed: ${error.message}`);

  const { data: pub } = supabase.storage.from(bucket).getPublicUrl(filePath);
  return pub.publicUrl;
}

/**
 * Runs Ghostscript separations for a single page.
 * Produces files like: plates-Cyan.tif, plates-Black.tif, plates-PANTONE 123 C.tif, etc
 */
async function extractSepsWithGhostscript(pdfFile, outDir, page) {
  await fsp.mkdir(outDir, { recursive: true });

  // Output template: one file per separation, name contains the separation name.
  // %s is separation name in many GS builds; if your GS behaves differently, we’ll adjust.
  const outPattern = path.join(outDir, "sep-%s.tif");

  const args = [
    "-q",
    "-dSAFER",
    "-dBATCH",
    "-dNOPAUSE",
    "-sDEVICE=tiffsep",
    "-r144",
    `-dFirstPage=${page}`,
    `-dLastPage=${page}`,
    `-sOutputFile=${outPattern}`,
    pdfFile,
  ];

  const { stderr } = await execFileAsync("gs", args, { maxBuffer: 1024 * 1024 * 10 });
  if (stderr?.trim()) {
    // GS often writes warnings to stderr; we won't treat as fatal automatically
    console.log("Ghostscript stderr:", stderr.trim());
  }

  // Collect TIFFs
  const files = (await fsp.readdir(outDir))
    .filter((f) => f.toLowerCase().endsWith(".tif") || f.toLowerCase().endsWith(".tiff"))
    .map((f) => path.join(outDir, f));

  if (files.length === 0) throw new Error("No separations produced by Ghostscript.");
  return files;
}

/**
 * Convert TIFF to PNG using ImageMagick if available.
 * If your Dockerfile doesn’t include ImageMagick, we can switch this to GraphicsMagick or sharp+tiff plugin,
 * but ImageMagick is the simplest.
 */
async function tiffToPng(tifPath, pngPath) {
  // "magick" on newer images, "convert" on older.
  // Try magick first, fall back to convert.
  try {
    await execFileAsync("magick", [tifPath, pngPath], { maxBuffer: 1024 * 1024 * 10 });
  } catch {
    await execFileAsync("convert", [tifPath, pngPath], { maxBuffer: 1024 * 1024 * 10 });
  }
}

function plateNameFromFilename(filePath) {
  // sep-Cyan.tif -> Cyan
  const base = path.basename(filePath);
  const m = base.match(/^sep-(.*)\.tiff?$/i);
  return m ? m[1] : base.replace(/\.(tiff?|png)$/i, "");
}

async function processJob(job) {
  const payload = job.payload || {};
  const bucket = payload.bucket || "separations";
  const pdfPath = payload.pdfPath;
  const proofPageId = payload.proofPageId || job.id; // fallback
  const page = Number(payload.page || 1);

  if (!pdfPath) throw new Error("Job payload missing pdfPath");

  const tmpRoot = await fsp.mkdtemp(path.join(os.tmpdir(), "plates-"));
  const pdfFile = path.join(tmpRoot, "input.pdf");
  const sepsDir = path.join(tmpRoot, "seps");
  const pngDir = path.join(tmpRoot, "png");

  await fsp.mkdir(pngDir, { recursive: true });

  console.log("⬇️ Downloading PDF:", bucket, pdfPath);
  await downloadFromBucket(bucket, pdfPath, pdfFile);

  console.log("🎯 Extracting separations (page", page, ")");
  const tiffs = await extractSepsWithGhostscript(pdfFile, sepsDir, page);

  const uploaded = [];
  for (const tif of tiffs) {
    const plateName = plateNameFromFilename(tif);
    const pngPath = path.join(pngDir, `${plateName}.png`);

    await tiffToPng(tif, pngPath);

    const buf = await fsp.readFile(pngPath);
    const outStoragePath = `separations/${proofPageId}/${plateName}.png`;

    const url = await uploadToBucket(bucket, outStoragePath, "image/png", buf);

    uploaded.push({ plate: plateName, url });
  }

  // Cleanup temp
  try {
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  } catch {}

  // Return something your app can use
  const result = {
    ok: true,
    proofPageId,
    page,
    pdfPath,
    plates: uploaded, // [{plate, url}]
    processed_at: new Date().toISOString(),
  };

  return result;
}

async function tryClaimOneJob() {
  const { data: jobs, error: findErr } = await supabase
    .from("plate_jobs")
    .select("id,payload,status")
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

    await supabase
      .from("plate_jobs")
      .update({ status: "failed", error: msg })
      .eq("id", job.id);
  }
}

async function loop() {
  await ensureTools();
  while (true) {
    await tryClaimOneJob();
    await sleep(1500);
  }
}

loop();
