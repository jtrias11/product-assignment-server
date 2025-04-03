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

// Logging Middleware
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
  next();
});

// File Paths
const DATA_DIR = path.join(__dirname, 'data');
const OUTPUT_CSV = path.join(DATA_DIR, 'output.csv');
const ROSTER_EXCEL = path.join(DATA_DIR, 'Walmart BH Roster.xlsx');

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

// Existing utility functions for data loading remain the same as in previous implementation

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