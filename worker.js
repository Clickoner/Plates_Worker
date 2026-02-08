import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing env vars. Need SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

console.log("Plates worker started ✅");

async function tryClaimOneJob() {
  // 1) find one queued job
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

  if (!jobs || jobs.length === 0) return; // nothing to do

  const job = jobs[0];

  // 2) lock it (so only one worker takes it)
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

  if (!locked) return; // someone else grabbed it

  console.log("✅ Picked up job:", job.id);

  // 3) do “fake work” for now
  const result = { ok: true, processed_at: new Date().toISOString(), payload: job.payload };

  // 4) mark done
  const { error: doneErr } = await supabase
    .from("plate_jobs")
    .update({ status: "done", result })
    .eq("id", job.id);

  if (doneErr) {
    console.error("Error marking done:", doneErr.message);
    // mark failed so it doesn't stay stuck forever
    await supabase
      .from("plate_jobs")
      .update({ status: "failed", error: doneErr.message })
      .eq("id", job.id);
    return;
  }

  console.log("🎉 Job done:", job.id);
}

async function loop() {
  while (true) {
    try {
      await tryClaimOneJob();
    } catch (e) {
      console.error("Worker loop error:", e?.message || e);
    }
    // wait 2 seconds between polls
    await new Promise((r) => setTimeout(r, 2000));
  }
}

loop();
