const express = require("express");
const { v4: uuidv4 } = require("uuid");

const app = express();
app.use(express.json());

const RATE_LIMIT_MS = 5000;
const BATCH_SIZE = 3;

let ingestionStore = {}; // { ingestion_id: { status, batches: [ ... ] } }
let jobQueue = [];       // [{ ids, priority, createdAt, ingestion_id, batch_id }]
let isProcessing = false;

// Priority Map
const PRIORITY_ORDER = {
  HIGH: 1,
  MEDIUM: 2,
  LOW: 3
};

// Mock fetch function
const mockFetch = (id) =>
  new Promise((resolve) =>
    setTimeout(() => resolve({ id, data: "processed" }), 500)
  );

// POST /ingest
app.post("/ingest", (req, res) => {
  const { ids, priority } = req.body;
  if (!ids || !priority || !Array.isArray(ids)) {
    return res.status(400).json({ error: "Invalid input" });
  }

  const ingestion_id = uuidv4();
  const createdAt = Date.now();
  ingestionStore[ingestion_id] = {
    ingestion_id,
    status: "yet_to_start",
    batches: []
  };

  // Break into batches of 3
  for (let i = 0; i < ids.length; i += BATCH_SIZE) {
    const batch_ids = ids.slice(i, i + BATCH_SIZE);
    const batch_id = uuidv4();
    ingestionStore[ingestion_id].batches.push({
      batch_id,
      ids: batch_ids,
      status: "yet_to_start"
    });

    jobQueue.push({
      ingestion_id,
      batch_id,
      ids: batch_ids,
      priority,
      createdAt
    });
  }

  jobQueue.sort((a, b) => {
    if (PRIORITY_ORDER[a.priority] !== PRIORITY_ORDER[b.priority]) {
      return PRIORITY_ORDER[a.priority] - PRIORITY_ORDER[b.priority];
    }
    return a.createdAt - b.createdAt;
  });

  if (!isProcessing) processQueue();

  return res.status(200).json({ ingestion_id });
});

// GET /status/:ingestion_id
app.get("/status/:ingestion_id", (req, res) => {
  const { ingestion_id } = req.params;
  const record = ingestionStore[ingestion_id];

  if (!record) return res.status(404).json({ error: "Ingestion ID not found" });

  const statuses = record.batches.map((b) => b.status);
  let overallStatus = "yet_to_start";
  if (statuses.every((s) => s === "completed")) overallStatus = "completed";
  else if (statuses.some((s) => s === "triggered")) overallStatus = "triggered";

  record.status = overallStatus;
  return res.status(200).json(record);
});

// Batch processor
async function processQueue() {
  isProcessing = true;

  while (jobQueue.length > 0) {
    const job = jobQueue.shift();

    // Mark as triggered
    const batch = ingestionStore[job.ingestion_id].batches.find(
      (b) => b.batch_id === job.batch_id
    );
    if (batch) batch.status = "triggered";

    // Simulate external processing
    await Promise.all(job.ids.map((id) => mockFetch(id)));

    // Mark as completed
    if (batch) batch.status = "completed";

    // Wait 5 seconds before next batch
    await new Promise((resolve) => setTimeout(resolve, RATE_LIMIT_MS));
  }

  isProcessing = false;
}

module.exports = app;
