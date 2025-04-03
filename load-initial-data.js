require('dotenv').config();
const fs = require('fs').promises;
const path = require('path');
const mongoose = require('mongoose');

// Import models
const Agent = require('./models/Agent');
const Product = require('./models/Product');
const Assignment = require('./models/Assignment');

const DATA_DIR = path.join(__dirname, 'data');
const AGENTS_FILE = path.join(DATA_DIR, 'agents.json');
const OUTPUT_CSV = path.join(DATA_DIR, 'output.csv');

async function readOutputCsv() {
  const csv = require('csv-parser');
  const { createReadStream } = require('fs');
  
  return new Promise((resolve) => {
    let results = [];
    console.log(`Looking for output CSV at: ${OUTPUT_CSV}`);
    
    try {
      createReadStream(OUTPUT_CSV)
        .pipe(csv())
        .on('data', (row) => results.push(row))
        .on('end', () => {
          console.log(`Loaded ${results.length} rows from output CSV.`);
          resolve(results);
        })
        .on('error', (error) => {
          console.error('Error reading output CSV:', error);
          resolve([]);
        });
    } catch (error) {
      console.error(`Output CSV file not found at: ${OUTPUT_CSV}`);
      resolve([]);
    }
  });
}

async function loadInitialData() {
  try {
    // Connect to MongoDB
    console.log("Connecting to MongoDB...");
    await mongoose.connect(process.env.MONGO_URI);
    console.log("Connected to MongoDB");
    
    // Check if agents already exist
    const agentCount = await Agent.countDocuments();
    if (agentCount === 0) {
      // Load agents from file
      try {
        console.log("No agents found in MongoDB, loading from file...");
        const agentsData = await fs.readFile(AGENTS_FILE, 'utf8');
        const agents = JSON.parse(agentsData);
        console.log(`Loaded ${agents.length} agents from file`);
        
        // Save agents to MongoDB
        await Agent.insertMany(agents);
        console.log(`Saved ${agents.length} agents to MongoDB`);
      } catch (error) {
        console.error("Error loading agents:", error);
      }
    } else {
      console.log(`Found ${agentCount} agents in MongoDB, skipping import`);
    }
    
    // Check if products already exist
    const productCount = await Product.countDocuments();
    if (productCount === 0) {
      // Load products from file
      try {
        console.log("No products found in MongoDB, loading from CSV...");
        const csvRows = await readOutputCsv();
        if (csvRows.length > 0) {
          const products = csvRows.map(row => ({
            id: row.abstract_product_id || row.item_abstract_product_id || row['item.abstract_product_id'] || row.product_id,
            name: row.product_name || "",
            priority: row.rule_priority || row.priority,
            tenantId: row.tenant_id,
            createdOn: row.oldest_created_on || row.sys_created_on || row.created_on,
            count: row.count,
            assigned: false
          })).filter(p => p.id);
          
          console.log(`Processed ${products.length} products from CSV`);
          
          // Save products to MongoDB in batches
          const batchSize = 100;
          for (let i = 0; i < products.length; i += batchSize) {
            const batch = products.slice(i, i + batchSize);
            await Product.insertMany(batch);
            console.log(`Saved batch ${Math.floor(i/batchSize) + 1} of products to MongoDB`);
          }
        }
      } catch (error) {
        console.error("Error loading products:", error);
      }
    } else {
      console.log(`Found ${productCount} products in MongoDB, skipping import`);
    }
    
    console.log("Data loading complete!");
    
  } catch (error) {
    console.error("Error during data loading:", error);
  } finally {
    await mongoose.disconnect();
    console.log("Disconnected from MongoDB");
  }
}

loadInitialData();