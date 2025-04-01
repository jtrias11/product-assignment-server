// server.js
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs').promises;
const path = require('path');
const csvParser = require('csv-parser');
const xlsx = require('xlsx');
const { createReadStream } = require('fs');
const multer = require('multer');
const Queue = require('bull');

const app = express();
const PORT = process.env.PORT || 3001;

// Enable CORS and JSON parsing
app.use(cors());
app.use(express.json());

// Setup Bull queue for CSV processing (if needed for other tasks)
const csvQueue = new Queue('csvProcessing', {
  redis: { host: '127.0.0.1', port: 6379 } // Adjust if needed
});

// Data storage for agents, products, and assignments
let agents = [];
let products = [];
let assignments = [];

// Lock for handling concurrent assignment requests
let assignmentInProgress = false;

// Data directories and file paths
const DATA_DIR = path.join(__dirname, 'data');
const AGENTS_FILE = path.join(DATA_DIR, 'agents.json');
const ASSIGNMENTS_FILE = path.join(DATA_DIR, 'assignments.json');
// Define the output CSV file that is now your product data source.
// (Assuming the file name is "data output.csv" in your project root.)
const OUTPUT_CSV = path.join(__dirname, 'data output.csv');
const ROSTER_EXCEL = path.join(DATA_DIR, 'Walmart BH Roster.xlsx');

// Configure Multer to save uploaded CSV files into DATA_DIR using original filename
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, DATA_DIR);
  },
  filename: function (req, file, cb) {
    cb(null, file.originalname);
  }
});
const upload = multer({ storage: storage });

// Ensure data directory exists
async function ensureDataDir() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    console.log('Data directory is ready');
  } catch (error) {
    console.error('Error creating data directory:', error);
  }
}

// Read products from the output CSV file
function readOutputCsv() {
  return new Promise((resolve, reject) => {
    let results = [];
    // Check if the output CSV file exists
    fs.access(OUTPUT_CSV)
      .then(() => {
        createReadStream(OUTPUT_CSV)
          .pipe(csvParser())
          .on('data', (data) => {
            results.push(data);
          })
          .on('end', () => {
            console.log(`Loaded ${results.length} products from output CSV.`);
            resolve(results);
          })
          .on('error', (error) => {
            console.error("Error reading output CSV:", error);
            resolve([]); // resolve empty if error
          });
      })
      .catch(() => {
        console.error(`Output CSV file not found at ${OUTPUT_CSV}`);
        resolve([]);
      });
  });
}

// Read Excel file for agent roster
async function readRosterExcel() {
  try {
    if (!await fileExists(ROSTER_EXCEL)) {
      console.log('Roster Excel file not found');
      return [];
    }
    const workbook = xlsx.readFile(ROSTER_EXCEL);
    console.log('Excel sheet names:', workbook.SheetNames);
    let sheetName = "Agents List";
    if (!workbook.SheetNames.includes(sheetName)) {
      console.log('Sheet "Agents List" not found, checking alternatives...');
      const possibleSheetNames = ["Agents", "AgentsList", "Agents_List", "Agent List", "Agent_List"];
      for (const name of possibleSheetNames) {
        if (workbook.SheetNames.includes(name)) {
          sheetName = name;
          console.log(`Found sheet "${name}" instead`);
          break;
        }
      }
      if (!workbook.SheetNames.includes(sheetName)) {
        sheetName = workbook.SheetNames[0];
        console.log(`Using first available sheet: "${sheetName}"`);
      }
    }
    const worksheet = workbook.Sheets[sheetName];
    const range = xlsx.utils.decode_range(worksheet['!ref']);
    console.log('Analyzing Excel sheet structure:');
    for (let c = 0; c <= Math.min(range.e.c, 10); c++) {
      const headerRef = xlsx.utils.encode_cell({ r: 0, c });
      const headerCell = worksheet[headerRef];
      if (headerCell) {
        console.log(`Column ${String.fromCharCode(65 + c)} (${c}): ${headerCell.v}`);
      }
    }
    const agentsList = [];
    // Extract agent names from column E (index 4), skipping header.
    for (let row = 1; row <= range.e.r; row++) {
      const cellRef = xlsx.utils.encode_cell({ r: row, c: 4 });
      const cell = worksheet[cellRef];
      if (cell && cell.v && typeof cell.v === 'string' && cell.v.trim() !== '') {
        const name = cell.v.trim();
        if (!['trimmed zoho name', 'name', 'agent name'].includes(name.toLowerCase())) {
          agentsList.push({
            id: agentsList.length + 1,
            name,
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

// Helper: Check if a file exists
async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

// Load data from files or initialize sample data.
// Now products are loaded from the output CSV.
async function loadData() {
  try {
    await ensureDataDir();
    
    // Load agents
    try {
      const agentsData = await fs.readFile(AGENTS_FILE, 'utf8');
      agents = JSON.parse(agentsData);
      console.log(`Loaded ${agents.length} agents from JSON file`);
    } catch (error) {
      console.log('No agents JSON file found, importing from Excel roster');
      const excelAgents = await readRosterExcel();
      if (excelAgents.length > 0) {
        agents = excelAgents;
        await saveAgents();
      } else {
        console.log('Excel import failed, using sample agent data');
        agents = [
          { id: 1, name: "Aaron Dale Yaeso Bandong", role: "Item Review", capacity: 10, currentAssignments: [] },
          { id: 2, name: "Aaron Marx Lenin Tuban Oriola", role: "Item Review", capacity: 10, currentAssignments: [] }
        ];
        await saveAgents();
      }
    }
    
    // Load products from the output CSV.
    try {
      const productsData = await readOutputCsv();
      // Map the CSV rows to product objects.
      // Expected columns: abstract_product_id, rule_priority, tenant_id, oldest_created_on, count
      products = productsData.map(row => ({
        id: row.abstract_product_id,
        // You might not have itemId or name here; set default values if needed.
        itemId: 0,
        name: "", 
        priority: row.rule_priority,
        createdOn: row.oldest_created_on,
        tenantId: row.tenant_id,
        count: row.count,
        assigned: false
      }));
      console.log(`Loaded ${products.length} products from output CSV`);
    } catch (error) {
      console.log('Error loading products from output CSV:', error);
      products = [];
    }
    
    // Load assignments
    try {
      const assignmentsData = await fs.readFile(ASSIGNMENTS_FILE, 'utf8');
      assignments = JSON.parse(assignmentsData);
      console.log(`Loaded ${assignments.length} assignments from file`);
      updateAgentAssignments();
    } catch (error) {
      console.log('No assignments file found, initializing with empty array');
      assignments = [];
      await saveAssignments();
    }
  } catch (error) {
    console.error('Error loading data:', error);
  }
}

// Update agents' currentAssignments based on assignments array
function updateAgentAssignments() {
  agents.forEach(agent => {
    agent.currentAssignments = [];
  });
  assignments.forEach(assignment => {
    const agent = agents.find(a => a.id === assignment.agentId);
    const product = products.find(p => p.id === assignment.productId);
    if (agent && product) {
      agent.currentAssignments.push({
        productId: product.id,
        name: product.name,
        priority: product.priority,
        tenantId: product.tenantId,
        createdOn: product.createdOn,
        count: product.count || 1,
        assignmentId: assignment.id
      });
    }
  });
}

// Save functions for agents and assignments (products are loaded from CSV output)
async function saveAgents() {
  try {
    await fs.writeFile(AGENTS_FILE, JSON.stringify(agents, null, 2));
    console.log('Agents saved to file');
  } catch (error) {
    console.error('Error saving agents:', error);
  }
}
async function saveAssignments() {
  try {
    await fs.writeFile(ASSIGNMENTS_FILE, JSON.stringify(assignments, null, 2));
    console.log('Assignments saved to file');
  } catch (error) {
    console.error('Error saving assignments:', error);
  }
}

// API Routes

// Health Check
app.get('/', (req, res) => {
  res.send('Product Assignment Server is running');
});
app.get('/api/agents', (req, res) => {
  res.json(agents);
});
app.get('/api/products', (req, res) => {
  res.json(products);
});
app.get('/api/assignments', (req, res) => {
  res.json(assignments);
});

// (Other endpoints for assignment, completion, unassignment remain unchanged)
// For brevity, only the refresh and upload endpoints are modified here.

// Upload endpoint: (if you still want to allow file upload to update output CSV)
// Here we allow multiple file uploads; note that this endpoint does not process CSVs into output.csv.
// You must update output.csv separately (or implement a background job) if needed.
app.post('/api/upload-csv', upload.array('files', 10), async (req, res) => {
  try {
    const uploadedFiles = req.files.map(f => f.originalname);
    console.log(`Uploaded files: ${uploadedFiles.join(', ')}`);
    // Here you might want to trigger your separate Python process or similar.
    // For now, we'll assume that output.csv is updated externally.
    res.status(200).json({ message: 'Files uploaded. Please run your CSV processing script to update output.csv.' });
  } catch (error) {
    console.error('Error uploading files:', error);
    res.status(500).json({ error: error.message });
  }
});

// Refresh endpoint: re-read the output CSV to update products in memory.
app.post('/api/refresh', async (req, res) => {
  try {
    await loadData();
    console.log('Data refreshed successfully from output CSV');
    res.status(200).json({ message: 'Data refreshed successfully' });
  } catch (error) {
    console.error('Error refreshing data:', error);
    res.status(500).json({ error: 'Failed to refresh data' });
  }
});

// (Other endpoints for assign, complete, unassign remain unchanged)
// Example: assign endpoint (unchanged)
app.post('/api/assign', async (req, res) => {
  if (assignmentInProgress) {
    return res.status(409).json({ error: 'Another assignment is in progress, please try again in a moment' });
  }
  assignmentInProgress = true;
  try {
    const { agentId } = req.body;
    if (!agentId) {
      assignmentInProgress = false;
      return res.status(400).json({ error: 'Agent ID is required' });
    }
    const agent = agents.find(a => a.id === agentId);
    if (!agent) {
      assignmentInProgress = false;
      return res.status(404).json({ error: 'Agent not found' });
    }
    if (agent.currentAssignments.length >= agent.capacity) {
      assignmentInProgress = false;
      return res.status(400).json({ error: 'Agent has reached maximum capacity' });
    }
    
    const assignedProductIds = new Set();
    agents.forEach(a => {
      a.currentAssignments.forEach(task => {
        assignedProductIds.add(task.productId);
      });
    });
    console.log(`Currently assigned product IDs: ${Array.from(assignedProductIds).join(', ')}`);
    
    const priorityOrder = { "P1": 0, "P2": 1, "P3": 2 };
    const availableProducts = products
      .filter(p => !p.assigned && !assignedProductIds.has(p.id))
      .sort((a, b) => {
        // Assuming createdOn is a string in the "YYYY-MM-DD HH:MM:SS" format
        return new Date(a.createdOn) - new Date(b.createdOn);
      });
    
    if (availableProducts.length === 0) {
      assignmentInProgress = false;
      return res.status(404).json({ error: 'No available products to assign' });
    }
    
    const productToAssign = availableProducts[0];
    productToAssign.assigned = true;
    
    const assignment = {
      id: uuidv4(),
      agentId: agent.id,
      productId: productToAssign.id,
      assignedOn: new Date().toISOString().replace('T', ' ').substring(0, 19)
    };
    assignments.push(assignment);
    agent.currentAssignments.push({
      productId: productToAssign.id,
      name: productToAssign.name,
      priority: productToAssign.priority,
      tenantId: productToAssign.tenantId,
      createdOn: productToAssign.createdOn,
      count: productToAssign.count || 1,
      assignmentId: assignment.id
    });
    
    console.log(`Assigned product ID ${productToAssign.id} to agent ${agent.name}`);
    
    await saveAssignments();
    await saveAgents();
    assignmentInProgress = false;
    res.status(200).json({ 
      message: `Task ${productToAssign.id} assigned to ${agent.name}`,
      assignment
    });
  } catch (error) {
    assignmentInProgress = false;
    console.error('Error assigning task:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// (Other endpoints for complete/unassign remain as before.)

// Start the server and load data initially.
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await loadData();
  console.log('Server is ready to handle requests');
});
