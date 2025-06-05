function processBatch(ids) {
  return new Promise((resolve) => {
    setTimeout(() => {
      ids.forEach(id => {
        console.log({ id, data: "processed" });
      });
      resolve();
    }, 1000); // simulate delay per batch
  });
}

module.exports = { processBatch };
