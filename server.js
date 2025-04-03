/***************************************************************
 * server.js - Enhanced MongoDB Integration
 * 
 * Features:
 * - Robust data loading from multiple sources
 * - Comprehensive CORS configuration
 * - Performance optimizations
 ***************************************************************/

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const path = require('path');
const fs = require('fs').promises;
const csvParser = require('csv-parser');
const xlsx = require('xlsx');
const { createReadStream } = require('fs');

// Model Imports
const Agent = require('./models/Agent');
const Product = require('./models/Product');
const Assignment = require('./models/Assignment');

// Configuration
const app = express();
const PORT = process.env.PORT || 3001;

// Enhanced CORS Configuration
const corsOptions = {
  origin: [
    'http://localhost:3000',  // Local development
    'https://product-assignment-frontend.onrender.com',  // Frontend deployment
    'https://product-assignment-server.onrender.com',  // Backend deployment
  ],
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: true,
  optionsSuccessStatus: 200
};

// Middleware
app.use(cors(corsOptions));
app.use(express.json());

// File Paths
const DATA_DIR = path.join(__dirname, 'data');
const OUTPUT_CSV = path.join(DATA_DIR, 'output.csv');
const ROSTER_EXCEL = path.join(DATA_DIR, 'Walmart BH Roster.xlsx');

// Utility Functions
async function ensureDataDir() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    console.log('Data directory ensured');
    return true;
  } catch (error) {
    console.error('Error creating data directory:', error);
    return false;
  }
}

// Read Agents from Excel
async function readRosterExcel() {
  try {
    // Check if file exists
    await fs.access(ROSTER_EXCEL);
    
    const workbook = xlsx.readFile(ROSTER_EXCEL);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const data = xlsx.utils.sheet_to_json(worksheet);
    
    const agents = data.map((row, index) => ({
      id: index + 1,
      name: row.Name || `Agent ${index + 1}`,
      role: "Item Review",
      capacity: 30,  // Updated capacity to 30
      currentAssignments: []
    }));
    
    console.log(`Extracted ${agents.length} agents from Excel`);
    return agents;
  } catch (error) {
    console.error('Error reading Excel roster:', error);
    return [];
  }
}

// Read Products from CSV
async function readOutputCsv() {
  return new Promise((resolve) => {
    // Check if file exists
    fs.access(OUTPUT_CSV)
      .then(() => {
        const results = [];
        
        createReadStream(OUTPUT_CSV)
          .pipe(csvParser())
          .on('data', (row) => {
            const productId = row.abstract_product_id || 
                              row.item_abstract_product_id || 
                              row['item.abstract_product_id'] || 
                              row.product_id;
            
            if (productId) {
              results.push({
                id: productId,
                name: row.product_name || '',
                priority: row.rule_priority || row.priority || 'P3',
                tenantId: row.tenant_id || '',
                createdOn: row.oldest_created_on || 
                           row.sys_created_on || 
                           row.created_on || 
                           new Date().toISOString(),
                count: row.count || 1,
                assigned: false
              });
            }
          })
          .on('end', () => {
            console.log(`Loaded ${results.length} products from CSV`);
            resolve(results);
          })
          .on('error', (error) => {
            console.error('Error reading output CSV:', error);
            resolve([]);
          });
      })
      .catch(() => {
        console.log('No output CSV file found');
        resolve([]);
      });
  });
}

// Create Sample Agents
function createSampleAgents() {
  const sampleAgents = [];
  for (let i = 1; i <= 10; i++) {
    sampleAgents.push({
      id: i,
      name: `Sample Agent ${i}`,
      role: "Item Review",
      capacity: 30,  // Updated capacity to 30
      currentAssignments: []
    });
  }
  return sampleAgents;
}

// Create Sample Products
function createSampleProducts() {
  const sampleProducts = [];
  for (let i = 1; i <= 20; i++) {
    sampleProducts.push({
      id: `SAMPLE${i.toString().padStart(5, '0')}`,
      name: `Sample Product ${i}`,
      priority: i % 3 === 0 ? 'P1' : (i % 3 === 1 ? 'P2' : 'P3'),
      tenantId: `Sample-Tenant-${Math.floor(i/5) + 1}`,
      createdOn: new Date(Date.now() - (i * 86400000)).toISOString(),
      count: Math.floor(Math.random() * 5) + 1,
      assigned: false
    });
  }
  return sampleProducts;
}

// Enhanced MongoDB Connection
const connectDB = async () => {
  try {
    await mongoose.connect(process.env.MONGO_URI, {
      serverSelectionTimeoutMS: 30000,
      socketTimeoutMS: 45000,
      connectTimeoutMS: 45000
    });
    
    console.log('MongoDB Connected Successfully');
    
    // Create indexes for performance
    await Promise.all([
      Agent.createIndexes(),
      Product.createIndexes(),
      Assignment.createIndexes()
    ]);
    
    return true;
  } catch (error) {
    console.error('MongoDB Connection Error:', error);
    return false;
  }
};

// Comprehensive Data Loading Function
async function loadData() {
  await ensureDataDir();

  // Load Agents
  let agentsFromDB = await Agent.find();
  if (agentsFromDB.length === 0) {
    console.log('No agents in database, attempting to load from sources');
    
    const excelAgents = await readRosterExcel();
    const agentsToSave = excelAgents.length > 0 ? excelAgents : createSampleAgents();
    
    await Agent.insertMany(agentsToSave);
    agentsFromDB = agentsToSave;
    console.log(`Saved ${agentsFromDB.length} agents to database`);
  }

  // Load Products
  let productsFromDB = await Product.find();
  if (productsFromDB.length === 0) {
    console.log('No products in database, attempting to load from sources');
    
    const csvProducts = await readOutputCsv();
    const productsToSave = csvProducts.length > 0 ? csvProducts : createSampleProducts();
    
    await Product.insertMany(productsToSave);
    productsFromDB = productsToSave;
    console.log(`Saved ${productsFromDB.length} products to database`);
  }
}

// API Routes
app.get('/', (req, res) => {
  res.json({ 
    message: 'Product Assignment Server is running',
    endpoints: [
      '/api/agents',
      '/api/products',
      '/api/assignments'
    ]
  });
});

app.get('/api/agents', async (req, res) => {
  try {
    const agents = await Agent.find();
    res.json(agents);
  } catch (error) {
    console.error('Error fetching agents:', error);
    res.status(500).json({ error: 'Failed to fetch agents' });
  }
});

app.get('/api/products', async (req, res) => {
  try {
    const products = await Product.find();
    res.json(products);
  } catch (error) {
    console.error('Error fetching products:', error);
    res.status(500).json({ error: 'Failed to fetch products' });
  }
});

app.get('/api/assignments', async (req, res) => {
  try {
    const assignments = await Assignment.find();
    res.json(assignments);
  } catch (error) {
    console.error('Error fetching assignments:', error);
    res.status(500).json({ error: 'Failed to fetch assignments' });
  }
});

// Error Handling Middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ 
    error: 'Something went wrong!',
    details: err.message 
  });
});

// Server Startup
async function startServer() {
  try {
    // Connect to MongoDB
    await connectDB();
    
    // Load initial data
    await loadData();
    
    // Start Express server
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`Server running on port ${PORT}`);
      console.log(`Listening on all network interfaces`);
    });
  } catch (error) {
    console.error('Server startup failed:', error);
    process.exit(1);
  }
}

// Initialize Server
startServer();

// Graceful Shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully');
  try {
    await mongoose.connection.close();
    process.exit(0);
  } catch (error) {
    console.error('Error during shutdown:', error);
    process.exit(1);
  }
});
