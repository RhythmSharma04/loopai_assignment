const { processBatch } = require("./processor");
const { updateBatchStatus } = require("./storage");

const queue = [];

const priorityMap = { HIGH: 1, MEDIUM: 2, LOW: 3 };

function enqueueJob(job) {
  queue.push(job);
  queue.sort((a, b) => {
    if (priorityMap[a.priority] !== priorityMap[b.priority]) {
      return priorityMap[a.priority] - priorityMap[b.priority];
    }
    return a.createdAt - b.createdAt;
  });
}

function startQueueProcessor() {
  setInterval(async () => {
    if (queue.length === 0) return;

    const job = queue.shift();
    updateBatchStatus(job.ingestion_id, job.batch_id, "triggered");

    await processBatch(job.ids);

    updateBatchStatus(job.ingestion_id, job.batch_id, "completed");
  }, 5000);
}

module.exports = { enqueueJob, startQueueProcessor };
