const request = require("supertest");
const app = require("../app");

describe("Ingestion System", () => {
  it("should accept an ingestion request", async () => {
    const res = await request(app)
      .post("/ingest")
      .send({ ids: [1, 2, 3, 4, 5], priority: "HIGH" });

    expect(res.body).toHaveProperty("ingestion_id");
  });

  it("should reject invalid ingestion requests", async () => {
    const res = await request(app).post("/ingest").send({ ids: "wrong" });
    expect(res.statusCode).toBe(400);
  });

  it("should return status", async () => {
    const res1 = await request(app)
      .post("/ingest")
      .send({ ids: [6, 7, 8], priority: "MEDIUM" });

    const ingestion_id = res1.body.ingestion_id;

    const res2 = await request(app).get(`/status/${ingestion_id}`);
    expect(res2.body).toHaveProperty("status");
    expect(res2.body).toHaveProperty("batches");
  });
});
