import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

console.log("Plates worker started");

async function pollJobs() {
  try {
    // 1. Claim one queued job
    const { data: job, error } = await supabase
      .from("plate_jobs")
      .update({
        status: "processing",
        locked_at: new Date().toISOString(),
      })
      .eq("status", "queued")
      .is("locked_at", null)
      .select()
      .limit(1)
      .single();

    if (!job || error) {
      return; // nothing to do
    }

    console.log("Picked up job:", job.id);

    // 2. Fake processing (replace later)
    await new Promise(r => setTimeout(r, 1000));

    // 3. Mark done
    await supabase
      .from("plate_jobs")
      .update({
        status: "done",
        result: { ok: true },
      })
      .eq("id", job.id);

    console.log("Job completed:", job.id);
  } catch (err) {
    console.error("Worker error:", err);
  }
}

// Poll every 2 seconds
setInterval(pollJobs, 2000);
