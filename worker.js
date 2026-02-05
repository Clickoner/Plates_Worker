console.log("Plates worker started");

// Keep the process alive so Render doesn't shut it down
setInterval(() => {
  console.log("Worker heartbeat", new Date().toISOString());
}, 30000);
