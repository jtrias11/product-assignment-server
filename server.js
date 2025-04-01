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
    'https://your-frontend-domain.com',  // Production frontend URL
    '*'  // Be cautious with this in production
  ],
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']
}));
app.use(express.json());

// Flexible directory configuration
const PRODUCT_DIRECTORY = process.env.PRODUCT_DIRECTORY || path.join(__dirname, 'data');
const ROSTER_FILE_PATH = process.env.ROSTER_FILE_PATH || path.join(__dirname, 'data', 'Walmart_BH_Roster.xlsx');

// Ensure data directory exists
fs.mkdirpSync(PRODUCT_DIRECTORY);

// In-memory storage (would use a database in production)
let products = [];
let agents = [];
let assignments = [];

// Function to load products from CSV
async function loadProductsFromCSV() {
  try {
    // Get list of CSV files
    const files = await fs.readdir(PRODUCT_DIRECTORY);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    if (csvFiles.length === 0) {
      console.log('No CSV files found');
      return [];
    }
    
    // Process all CSV files
    const allProducts = [];
    
    for (const file of csvFiles) {
      const filePath = path.join(PRODUCT_DIRECTORY, file);
      console.log(`Processing file: ${file}`);
      
      // Process each CSV file
      const results = await new Promise((resolve, reject) => {
        const fileResults = [];
        
        fs.createReadStream(filePath)
          .pipe(csvParser())
          .on('data', (data) => fileResults.push(data))
          .on('end', () => {
            console.log(`Finished processing ${file}`);
            resolve(fileResults);
          })
          .on('error', (error) => {
            console.error(`Error reading ${file}:`, error);
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
    
    return allProducts;
  } catch (error) {
    console.error('Error loading products:', error);
    return [];
  }
}

// Function to load agents from Excel
async function loadAgentsFromExcel() {
  try {
    if (!await fs.exists(ROSTER_FILE_PATH)) {
      console.log('Roster file not found');
      return [];
    }
    
    console.log(`Processing roster file: ${ROSTER_FILE_PATH}`);
    
    // Read the Excel file
    const workbook = XLSX.readFile(ROSTER_FILE_PATH);
    
    // Check if "Agents List" sheet exists
    if (!workbook.SheetNames.includes('Agents List')) {
      console.log('Agents List sheet not found');
      return [];
    }
    
    // Get the worksheet
    const worksheet = workbook.Sheets['Agents List'];
    
    // Convert to JSON
    const data = XLSX.utils.sheet_to_json(worksheet);
    
    // Process agents
    return data
      .filter(row => row['Status'] === 'Active')
      .map((row, index) => ({
        id: index + 1,
        name: row['Zoho Name'] || 'Unknown Agent',
        role: row['Role'] || 'Item Review',
        capacity: 10,
        currentAssignments: []
      }));
  } catch (error) {
    console.error('Error loading agents:', error);
    return [];
  }
}

// API endpoints
app.get('/api/products', async (req, res) => {
  try {
    // Dynamically load products each time
    products = await loadProductsFromCSV();
    res.json(products);
  } catch (error) {
    res.status(500).json({ error: 'Failed to load products' });
  }
});

app.get('/api/agents', async (req, res) => {
  try {
    // Dynamically load agents each time
    agents = await loadAgentsFromExcel();
    res.json(agents);
  } catch (error) {
    res.status(500).json({ error: 'Failed to load agents' });
  }
});

app.get('/api/assignments', (req, res) => {
  res.json(assignments);
});

// Manual refresh endpoint
app.post('/api/refresh-data', async (req, res) => {
  try {
    products = await loadProductsFromCSV();
    agents = await loadAgentsFromExcel();
    
    res.json({ 
      success: true, 
      message: 'Data refreshed successfully',
      productCount: products.length,
      agentCount: agents.length
    });
  } catch (error) {
    console.error('Error refreshing data:', error);
    res.status(500).json({ 
      success: false, 
      message: 'Failed to refresh data',
      error: error.toString()
    });
  }
});

// Add assignment and completion endpoints here (from previous implementation)

// Root endpoint for health check
app.get('/', (req, res) => {
  res.json({
    status: 'Server is running',
    timestamp: new Date().toISOString(),
    environment: process.env.NODE_ENV || 'development'
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
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
  console.log(`Server running on:`);
  console.log(`- http://localhost:${PORT}`);
  console.log(`- http://${HOST}:${PORT}`);
});

module.exports = { app, server }; // Export for potential testing