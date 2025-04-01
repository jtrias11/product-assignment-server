// worker.js
const Queue = require('bull');
const { loadData } = require('./server'); // Export loadData from your server.js or create a module for CSV processing

// Create a Bull queue named "csvProcessing"
const csvQueue = new Queue('csvProcessing', {
  redis: { host: '127.0.0.1', port: 6379 } // adjust as needed for your Redis instance
});

// Process jobs in the queue
csvQueue.process(async (job) => {
  console.log(`Processing job: ${job.id} for files: ${job.data.files}`);
  // Call your loadData function (which re-reads CSV files) or your CSV processing logic
  await loadData();
  console.log(`Job ${job.id} completed`);
  return { status: 'completed' };
});

// Optional: Listen for events
csvQueue.on('completed', (job, result) => {
  console.log(`Job ${job.id} completed with result:`, result);
});
