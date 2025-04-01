const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs').promises;
const path = require('path');
const csv = require('csv-parser');
const xlsx = require('xlsx');
const { createReadStream } = require('fs');

const app = express();
const PORT = process.env.PORT || 3001;

// Enable CORS for all routes
app.use(cors());
app.use(express.json());

// Data storage
let agents = [];
let products = [];
let assignments = [];

// Lock for handling concurrent assignment requests
let assignmentInProgress = false;

// Data directories and files
const DATA_DIR = path.join(__dirname, 'data');
const AGENTS_FILE = path.join(DATA_DIR, 'agents.json');
const PRODUCTS_FILE = path.join(DATA_DIR, 'products.json');
const ASSIGNMENTS_FILE = path.join(DATA_DIR, 'assignments.json');
const ROSTER_EXCEL = path.join(DATA_DIR, 'Walmart BH Roster.xlsx');

// Ensure data directory exists
async function ensureDataDir() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    console.log('Data directory is ready');
  } catch (error) {
    console.error('Error creating data directory:', error);
  }
}

// Function to read CSV files from the data directory
function readCsvFiles() {
  return new Promise(async (resolve, reject) => {
    try {
      const files = await fs.readdir(DATA_DIR);
      const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
      
      if (csvFiles.length === 0) {
        console.log('No CSV files found in data directory');
        return resolve([]);
      }
      
      let allProducts = [];
      let completedFiles = 0;
      let abstractIdCounts = {}; // To track counts of each abstract ID
      let skippedRows = 0; // To track how many rows were skipped
      
      for (const file of csvFiles) {
        const filePath = path.join(DATA_DIR, file);
        const fileProducts = [];
        
        createReadStream(filePath)
          .pipe(csv())
          .on('data', (data) => {
            // Log column names from the first row to help with debugging
            if (fileProducts.length === 0) {
              console.log(`CSV column names in ${file}:`, Object.keys(data));
            }
            
            // Get Abstract Product ID from item.abstract_product_id
            let productId = data['item.abstract_product_id'] || null;
            
            // If we couldn't find it directly, try alternative field names
            if (!productId) {
              if (data.item && data.item.abstract_product_id) {
                productId = data.item.abstract_product_id;
              } else if (data.abstract_product_id) {
                productId = data.abstract_product_id;
              } else if (data.AbstractID) {
                productId = data.AbstractID;
              } else if (data['Abstract ID']) {
                productId = data['Abstract ID'];
              } else if (data.abstract_id) {
                productId = data.abstract_id;
              }
            }
            
            // Skip rows with blank Abstract IDs
            if (!productId || productId.trim() === '') {
              skippedRows++;
              if (skippedRows % 100 === 0) {
                console.log(`Skipped ${skippedRows} rows with blank Abstract ID`);
              }
              return; // Skip this row
            }
            
            // Track counts of each abstract ID
            abstractIdCounts[productId] = (abstractIdCounts[productId] || 0) + 1;
            
            // Get priority from rule.priority
            const priority = data['rule.priority'] || data.priority || 'P3';
            
            // Get tenant ID
            const tenantId = data.tenant_id || data.TenantID || data['Tenant ID'] || '';
            
            // Get created date from sys_created_on
            const createdOn = data.sys_created_on || data.created_on || data.CreatedOn || 
                             new Date().toISOString().replace('T', ' ').substring(0, 19);
            
            // Get item name for reference
            const name = data.ItemName || data.Name || data.name || data.Description || 'Unknown Product';
            
            // Get item ID for reference
            const itemId = parseInt(data.ItemID || data.item_id || 0);
            
            const product = {
              id: productId,
              itemId: itemId,
              name: name,
              priority: priority,
              createdOn: createdOn,
              tenantId: tenantId,
              assigned: false
            };
            fileProducts.push(product);
          })
          .on('end', () => {
            console.log(`Read ${fileProducts.length} products from ${file}`);
            allProducts = [...allProducts, ...fileProducts];
            completedFiles++;
            
            if (completedFiles === csvFiles.length) {
              // Add count to each product
              allProducts.forEach(product => {
                product.count = abstractIdCounts[product.id] || 1;
              });
              
              console.log(`Total products loaded from CSVs: ${allProducts.length}`);
              console.log(`Total rows skipped (blank Abstract ID): ${skippedRows}`);
              resolve(allProducts);
            }
          })
          .on('error', (error) => {
            console.error(`Error reading CSV file ${file}:`, error);
            completedFiles++;
            if (completedFiles === csvFiles.length) {
              resolve(allProducts);
            }
          });
      }
    } catch (error) {
      console.error('Error reading CSV files:', error);
      resolve([]);
    }
  });
}

// Function to read Excel roster file specifically looking for the "Agents List" sheet and column E
async function readRosterExcel() {
  try {
    if (!await fileExists(ROSTER_EXCEL)) {
      console.log('Roster Excel file not found');
      return [];
    }
    
    const workbook = xlsx.readFile(ROSTER_EXCEL);
    
    // Log all sheet names for debugging
    console.log('Excel sheet names:', workbook.SheetNames);
    
    // Look for a sheet named "Agents List", or use the first sheet if not found
    let sheetName = "Agents List";
    if (!workbook.SheetNames.includes(sheetName)) {
      console.log('Sheet "Agents List" not found, checking other sheet names...');
      // Try alternative names
      const possibleSheetNames = ["Agents", "AgentsList", "Agents_List", "Agent List", "Agent_List"];
      for (const name of possibleSheetNames) {
        if (workbook.SheetNames.includes(name)) {
          sheetName = name;
          console.log(`Found sheet "${name}" instead`);
          break;
        }
      }
      
      // If still not found, use first sheet
      if (!workbook.SheetNames.includes(sheetName)) {
        sheetName = workbook.SheetNames[0];
        console.log(`Using first available sheet: "${sheetName}"`);
      }
    }
    
    const worksheet = workbook.Sheets[sheetName];
    
    // Get the range of the worksheet
    const range = xlsx.utils.decode_range(worksheet['!ref']);
    
    // Log the first few cells to understand the structure
    console.log('Analyzing Excel sheet structure:');
    for (let c = 0; c <= Math.min(range.e.c, 10); c++) {
      const headerRef = xlsx.utils.encode_cell({ r: 0, c: c });
      const headerCell = worksheet[headerRef];
      if (headerCell) {
        console.log(`Column ${String.fromCharCode(65 + c)} (${c}): ${headerCell.v}`);
      }
    }
    
    const agentsList = [];
    
    // Skip the first row (index 0) to avoid the header "Trimmed Zoho Name"
    // Loop through rows and extract names from column E (which is index 4)
    // Start from row 2 (index 1) to skip the header
    for (let row = 1; row <= range.e.r; row++) {
      const cellRef = xlsx.utils.encode_cell({ r: row, c: 4 }); // Column E (index 4)
      const cell = worksheet[cellRef];
      
      // Check if cell exists and has a value
      if (cell && cell.v && typeof cell.v === 'string' && cell.v.trim() !== '') {
        // Check if the value isn't the header (double check)
        const name = cell.v.trim();
        if (name.toLowerCase() !== 'trimmed zoho name' && 
            name.toLowerCase() !== 'name' &&
            name.toLowerCase() !== 'agent name') {
          agentsList.push({
            id: agentsList.length + 1,
            name: name,
            role: "Item Review",
            capacity: 10,
            currentAssignments: []
          });
        }
      }
    }
    
    console.log(`Read ${agentsList.length} agents from Excel roster (column E, skipping header)`);
    return agentsList;
  } catch (error) {
    console.error('Error reading Excel roster:', error);
    return [];
  }
}

// Helper function to check if a file exists
async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

// Load data from files or initialize with imported/sample data
async function loadData() {
  try {
    await ensureDataDir();
    
    // Try to load agents from JSON file first
    try {
      const agentsData = await fs.readFile(AGENTS_FILE, 'utf8');
      agents = JSON.parse(agentsData);
      console.log(`Loaded ${agents.length} agents from JSON file`);
    } catch (error) {
      console.log('No agents JSON file found, trying to import from Excel roster');
      
      // Try to import from Excel roster
      const excelAgents = await readRosterExcel();
      
      if (excelAgents.length > 0) {
        agents = excelAgents;
        await saveAgents();
      } else {
        console.log('Excel import failed, initializing with sample agent data');
        // Sample agents as fallback
        agents = [
          { id: 1, name: "Aaron Dale Yaeso Bandong", role: "Item Review", capacity: 10, currentAssignments: [] },
          { id: 2, name: "Aaron Marx Lenin Tuban Oriola", role: "Item Review", capacity: 10, currentAssignments: [] },
        ];
        await saveAgents();
      }
    }
    
    // Try to load products from JSON file first
    try {
      const productsData = await fs.readFile(PRODUCTS_FILE, 'utf8');
      products = JSON.parse(productsData);
      console.log(`Loaded ${products.length} products from JSON file`);
    } catch (error) {
      console.log('No products JSON file found, trying to import from CSV files');
      
      // Try to import from CSV files
      const csvProducts = await readCsvFiles();
      
      if (csvProducts.length > 0) {
        products = csvProducts;
        await saveProducts();
      } else {
        console.log('CSV import failed, initializing with sample product data');
        // Generate sample products as fallback
        products = [];
        for (let i = 0; i < 20; i++) {
          const priority = i % 3 === 0 ? "P1" : (i % 3 === 1 ? "P2" : "P3");
          const itemId = 15847610000 + i;
          products.push({
            id: `SAMPLE${i.toString().padStart(5, '0')}`,
            itemId,
            name: `Sample Product ${i+1} - ${["Sweater", "Jeans", "T-Shirt", "Jacket", "Dress"][i % 5]} Item`,
            priority,
            tenantId: `TEN
