const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs-extra');
const csvParser = require('csv-parser');
const XLSX = require('xlsx');

const app = express();

// Configure CORS for development and production
app.use(cors({
  origin: [
    'http://localhost:3000',  // Local React frontend
    'https://product-assignment-frontend.onrender.com',  // Production frontend
    '*'  // Be cautious with this in production
  ],
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']
}));
app.use(express.json());

// Flexible directory configuration with absolute path
const PRODUCT_DIRECTORY = path.join(__dirname, 'data');
const ROSTER_FILE_PATH = path.join(__dirname, 'data', 'Walmart BH Roster.xlsx');

// Ensure data directory exists
fs.mkdirpSync(PRODUCT_DIRECTORY);

// Comprehensive logging utility
function log(message, data = null) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${message}`, data ? data : '');
}

// In-memory storage (would use a database in production)
let products = [];
let agents = [];
let assignments = [];

// Function to load products from CSV
async function loadProductsFromCSV() {
  try {
    log('Starting CSV product loading process');
    log('Product Directory:', PRODUCT_DIRECTORY);

    // Validate directory
    if (!await fs.exists(PRODUCT_DIRECTORY)) {
      log('ERROR: Product directory does not exist');
      return [];
    }

    // Get list of CSV files
    const files = await fs.readdir(PRODUCT_DIRECTORY);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    log('CSV Files found:', csvFiles);

    if (csvFiles.length === 0) {
      log('No CSV files found in directory');
      return [];
    }
    
    // Process all CSV files
    const allProducts = [];
    
    for (const file of csvFiles) {
      const filePath = path.join(PRODUCT_DIRECTORY, file);
      log(`Processing file: ${file}`);
      
      // Process each CSV file
      const results = await new Promise((resolve, reject) => {
        const fileResults = [];
        
        fs.createReadStream(filePath)
          .pipe(csvParser())
          .on('data', (data) => fileResults.push(data))
          .on('end', () => {
            log(`Finished processing ${file}`);
            resolve(fileResults);
          })
          .on('error', (error) => {
            log(`Error reading ${file}:`, error);
            reject(error);
          });
      });
      
      // Process results from this file
      const processedResults = results
        .filter(row => row['item.abstract_product_id'])
        .map(row => ({
          id: row['item.abstract_product_id'],
          itemId: parseFloat(row['item.item_id']) || 0,
          name: row['item'] || 'Unknown Item',
          priority: row['rule.priority'] || 'P3',
          createdOn: row['sys_created_on'] || new Date().toISOString(),
          assigned: false,
          sourceFile: file
        }));
      
      allProducts.push(...processedResults);
    }
    
    // Sort all products by creation date
    allProducts.sort((a, b) => {
      const dateA = new Date(a.createdOn || 0);
      const dateB = new Date(b.createdOn || 0);
      return dateA - dateB;
    });
    
    log(`Processed ${allProducts.length} total products`);
    return allProducts;
  } catch (error) {
    log('CRITICAL ERROR in loadProductsFromCSV:', error);
    return [];
  }
}

// Function to load agents from Excel
async function loadAgentsFromExcel() {
  try {
    log('Starting Excel agent loading process');
    log('Roster File Path:', ROSTER_FILE_PATH);

    if (!await fs.exists(ROSTER_FILE_PATH)) {
      log('ERROR: Roster file not found');
      return [];
    }
    
    // Read the Excel file
    const workbook = XLSX.readFile(ROSTER_FILE_PATH);
    
    // Check if "Agents List" sheet exists
    if (!workbook.SheetNames.includes('Agents List')) {
      log('ERROR: Agents List sheet not found');
      return [];
    }
    
    // Get the worksheet
    const worksheet = workbook.Sheets['Agents List'];
    
    // Convert to JSON
    const data = XLSX.utils.sheet_to_json(worksheet);
    
    // Process agents
    const processedAgents = data
      .filter(row => row['Status'] === 'Active')
      .map((row, index) => ({
        id: index + 1,
        name: row['Zoho Name'] || 'Unknown Agent',
        role: row['Role'] || 'Item Review',
        capacity: 10,
        currentAssignments: []
      }));
    
    log(`Processed ${processedAgents.length} agents`);
    return processedAgents;
  } catch (error) {
    log('CRITICAL ERROR in loadAgentsFromExcel:', error);
    return [];
  }
}

// API endpoints with comprehensive error handling
app.get('/api/products', async (req, res) => {
  try {
    log('Products API endpoint called');
    products = await loadProductsFromCSV();
    res.json(products);
  } catch (error) {
    log('Error in products endpoint:', error);
    res.status(500).json({ 
      error: 'Failed to load products', 
      details: error.toString() 
    });
  }
});

app.get('/api/agents', async (req, res) => {
  try {
    log('Agents API endpoint called');
    agents = await loadAgentsFromExcel();
    res.json(agents);
  } catch (error) {
    log('Error in agents endpoint:', error);
    res.status(500).json({ 
      error: 'Failed to load agents', 
      details: error.toString() 
    });
  }
});

app.get('/api/assignments', (req, res) => {
  res.json(assignments);
});

// Manual refresh endpoint
app.post('/api/refresh-data', async (req, res) => {
  try {
    log('Manual data refresh initiated');
    products = await loadProductsFromCSV();
    agents = await loadAgentsFromExcel();
    
    res.json({ 
      success: true, 
      message: 'Data refreshed successfully',
      productCount: products.length,
      agentCount: agents.length
    });
  } catch (error) {
    log('Error during manual refresh:', error);
    res.status(500).json({ 
      success: false, 
      message: 'Failed to refresh data',
      error: error.toString()
    });
  }
});

// Root endpoint for health check
app.get('/', (req, res) => {
  res.json({
    status: 'Server is running',
    timestamp: new Date().toISOString(),
    environment: process.env.NODE_ENV || 'development',
    productDirectory: PRODUCT_DIRECTORY,
    rosterFilePath: ROSTER_FILE_PATH
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  log('Unhandled Error:', err);
  res.status(500).json({ 
    error: 'Something went wrong!',
    message: err.message 
  });
});

// Flexible port configuration
const PORT = process.env.PORT || 3001;
const HOST = process.env.HOST || '0.0.0.0';

// Start the server
const server = app.listen(PORT, HOST, () => {
  log(`Server running on:`);
  log(`- http://localhost:${PORT}`);
  log(`- http://${HOST}:${PORT}`);
});

module.exports = { app, server }; // Export for potential testing