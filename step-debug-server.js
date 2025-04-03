// Existing startup code with logging
console.log("Starting step debug server...");
require('dotenv').config();
console.log("Loaded environment variables");

// Load all modules and setup Express
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs').promises;
const path = require('path');
const csvParser = require('csv-parser');
const xlsx = require('xlsx');
const { createReadStream, createWriteStream } = require('fs');
const multer = require('multer');
const { format } = require('@fast-csv/format');
const mongoose = require('mongoose');
console.log("Loaded all required modules");

// Load models
const Agent = require('./models/Agent');
const Product = require('./models/Product');
const Assignment = require('./models/Assignment');
console.log("Loaded all models");

// Create Express app
const app = express();
const PORT = process.env.PORT || 3001;
app.use(cors());
app.use(express.json());
console.log("Set up Express middleware");

// Basic variables 
let agents = [];
let products = [];
let assignments = [];
const DATA_DIR = path.join(__dirname, 'data');

// MongoDB Connection
const connectDB = async () => {
  console.log("Attempting to connect to MongoDB...");
  try {
    await mongoose.connect(process.env.MONGO_URI);
    console.log('MongoDB Connected!');
    return true;
  } catch (error) {
    console.error('MongoDB Connection Error:', error);
    return false;
  }
};

// Simplified ensureDataDir function
async function ensureDataDir() {
  console.log("Ensuring data directory exists...");
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    console.log('Data directory is ready');
    return true;
  } catch (error) {
    console.error('Error creating data directory:', error);
    return false;
  }
}

// Simplified loadData function
async function loadData() {
  console.log("Starting to load data...");
  try {
    console.log("Checking data directory...");
    await ensureDataDir();
    
    console.log("Loading agents from MongoDB...");
    agents = await Agent.find();
    console.log(`Loaded ${agents.length} agents from MongoDB`);
    
    console.log("Loading products from MongoDB...");
    products = await Product.find();
    console.log(`Loaded ${products.length} products from MongoDB`);
    
    console.log("Loading assignments from MongoDB...");
    assignments = await Assignment.find();
    console.log(`Loaded ${assignments.length} assignments from MongoDB`);
    
    console.log("All data loaded successfully");
    return true;
  } catch (error) {
    console.error('Error loading data:', error);
    return false;
  }
}

// Define a basic route
app.get('/', (req, res) => {
  res.send('Step debug server is running');
});

// Start server
console.log("Starting Express server...");
const server = app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  
  console.log("Now connecting to MongoDB...");
  try {
    await connectDB();
    console.log("Now loading data...");
    await loadData();
    console.log("Server startup complete");
  } catch (error) {
    console.error("Error during startup:", error);
  }
});

server.on('error', (error) => {
  console.error('Server error:', error);
});