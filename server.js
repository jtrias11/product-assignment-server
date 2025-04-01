const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs-extra');
const csvParser = require('csv-parser');
const XLSX = require('xlsx');

const app = express();

// Configure CORS for all environments
app.use(cors({
  origin: '*', // Adjust in production
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']
}));
app.use(express.json());

// Use environment variables for configuration
const PRODUCT_DIRECTORY = process.env.PRODUCT_DIRECTORY || './data';
const ROSTER_FILE_PATH = process.env.ROSTER_FILE_PATH || './data/Walmart_BH_Roster.xlsx';

// Ensure data directory exists
fs.mkdirpSync(PRODUCT_DIRECTORY);

// Rest of your server.js code remains the same

// Function to load products from CSV
async function loadProductsFromCSV() {
  try {
    // Get list of CSV files
    const files = await fs.readdir(productDirectory);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    if (csvFiles.length === 0) {
      console.log('No CSV files found');
      return [];
    }
    
    // Process all CSV files
    const allProducts = [];
    
    for (const file of csvFiles) {
      const filePath = path.join(productDirectory, file);
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
    if (!await fs.exists(rosterFilePath)) {
      console.log('Roster file not found');
      return [];
    }
    
    console.log(`Processing roster file: ${rosterFilePath}`);
    
    // Read the Excel file
    const workbook = XLSX.readFile(rosterFilePath);
    
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

// Assignment and completion endpoints remain the same as in previous implementation

// Root endpoint for health check
app.get('/', (req, res) => {
  res.json({
    status: 'Server is running',
    timestamp: new Date().toISOString()
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

// Dynamic port for Heroku
const PORT = process.env.PORT || 3001;

// Start the server
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
});

module.exports = app; // For potential testing