const data = {};

function storeIngestion(ingestion_id, batches) {
  data[ingestion_id] = { batches };
}

function getIngestionStatus(ingestion_id) {
  return data[ingestion_id];
}

function updateBatchStatus(ingestion_id, batch_id, status) {
  const ingestion = data[ingestion_id];
  if (!ingestion) return;
  const batch = ingestion.batches.find(b => b.batch_id === batch_id);
  if (batch) batch.status = status;
}

module.exports = { storeIngestion, getIngestionStatus, updateBatchStatus };
