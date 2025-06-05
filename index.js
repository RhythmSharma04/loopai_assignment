const express = require("express");
const { v4: uuidv4 } = require("uuid");
const { enqueueJob, startQueueProcessor } = require("./queue");
const { storeIngestion, getIngestionStatus } = require("./storage");

const app = express();
const PORT = process.env.PORT || 5000;

app.use(express.json());

app.post("/ingest", (req, res) => {
  const { ids, priority } = req.body;

  if (!ids || !Array.isArray(ids) || !priority || !["HIGH", "MEDIUM", "LOW"].includes(priority)) {
    return res.status(400).json({ error: "Invalid input" });
  }

  const ingestion_id = uuidv4();
  const batches = [];

  for (let i = 0; i < ids.length; i += 3) {
    const batch_ids = ids.slice(i, i + 3);
    const batch_id = uuidv4();
    batches.push({ batch_id, ids: batch_ids, status: "yet_to_start" });
    enqueueJob({ ingestion_id, batch_id, ids: batch_ids, priority, createdAt: Date.now() });
  }

  storeIngestion(ingestion_id, batches);

  res.json({ ingestion_id });
});

app.get("/status/:ingestion_id", (req, res) => {
  const data = getIngestionStatus(req.params.ingestion_id);
  if (!data) return res.status(404).json({ error: "Ingestion not found" });

  const statuses = data.batches.map(b => b.status);
  let status = "yet_to_start";
  if (statuses.every(s => s === "completed")) status = "completed";
  else if (statuses.some(s => s === "triggered" || s === "completed")) status = "triggered";

  res.json({ ingestion_id: req.params.ingestion_id, status, batches: data.batches });
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  startQueueProcessor(); // start the processor loop
});
