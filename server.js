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
// Bull is used if you need asynchronous jobs (optional here)
const Queue = require('bull');

const app = express();
const PORT = process.env.PORT || 3001;

// Enable CORS and JSON parsing
app.use(cors());
app.use(express.json());

// (Optional) Setup Bull queue if needed
const csvQueue = new Queue('csvProcessing', {
  redis: { host: '127.0.0.1', port: 6379 } // Update if necessary
});

// ------------------------------
// Data Storage
// ------------------------------
let agents = [];
let products = [];
let assignments = [];

// ------------------------------
// File Paths and Directories
// ------------------------------
const DATA_DIR = path.join(__dirname, 'data');
const AGENTS_FILE = path.join(DATA_DIR, 'agents.json');
const ASSIGNMENTS_FILE = path.join(DATA_DIR, 'assignments.json');

// The output CSV produced by your separate Python script
const OUTPUT_CSV = path.join(__dirname, 'data output.csv');

// Roster Excel file for agents
const ROSTER_EXCEL = path.join(DATA_DIR, 'Walmart BH Roster.xlsx');

// ------------------------------
// Multer configuration for file uploads (if needed)
// ------------------------------
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, DATA_DIR);
  },
  filename: (req, file, cb) => {
    cb(null, file.originalname);
  }
});
const upload = multer({ storage });

// ------------------------------
// Helper Functions
// ------------------------------
async function ensureDataDir() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    console.log('Data directory is ready');
  } catch (error) {
    console.error('Error creating data directory:', error);
  }
}

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

// Load products from the output CSV file.
function readOutputCsv() {
  return new Promise((resolve, reject) => {
    let results = [];
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
            console.error('Error reading output CSV:', error);
            resolve([]);
          });
      })
      .catch(() => {
        console.error(`Output CSV file not found at ${OUTPUT_CSV}`);
        resolve([]);
      });
  });
}

// Load agents from the roster Excel file (using column E).
async function readRosterExcel() {
  try {
    if (!(await fileExists(ROSTER_EXCEL))) {
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
    // Loop through rows (starting at row index 1 to skip header) and get data from column E (index 4)
    for (let row = 1; row <= range.e.r; row++) {
      const cellRef = xlsx.utils.encode_cell({ r: row, c: 4 });
      const cell = worksheet[cellRef];
      if (cell && cell.v && typeof cell.v === 'string' && cell.v.trim() !== '') {
        const name = cell.v.trim();
        // Ignore header-like rows if present
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
    console.log(`Read ${agentsList.length} agents from Excel roster (column E)`);
    return agentsList;
  } catch (error) {
    console.error('Error reading Excel roster:', error);
    return [];
  }
}

// ------------------------------
// Load Data
// ------------------------------
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

    // Load products from output CSV.
    try {
      const productsData = await readOutputCsv();
      // Map each row from output CSV to a product object.
      // Expected CSV columns: abstract_product_id, rule_priority, tenant_id, oldest_created_on, count
      products = productsData.map(row => ({
        id: row.abstract_product_id,
        // You can assign default values for missing fields if needed.
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

// ------------------------------
// Save Functions
// ------------------------------
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

// ------------------------------
// API Routes
// ------------------------------
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

// Refresh endpoint: re-read the output CSV (and agents/assignments as usual)
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

// (Optional) Upload endpoint – if you want to allow file uploads to update CSVs.
// This example simply accepts file uploads; your process_csv.py script would be run externally.
app.post('/api/upload-csv', upload.array('files', 10), async (req, res) => {
  try {
    const uploadedFiles = req.files.map(f => f.originalname);
    console.log(`Uploaded files: ${uploadedFiles.join(', ')}`);
    // Here you could trigger an external process to regenerate output CSV.
    res.status(200).json({ message: 'Files uploaded. Please update output CSV externally.' });
  } catch (error) {
    console.error('Error uploading files:', error);
    res.status(500).json({ error: error.message });
  }
});

// Example assign endpoint (unchanged)
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
    
    // Sort available products by createdOn (oldest first)
    const availableProducts = products
      .filter(p => !p.assigned && !assignedProductIds.has(p.id))
      .sort((a, b) => new Date(a.createdOn) - new Date(b.createdOn));
    
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

// (Other endpoints like complete/unassign remain similar as needed.)

// ------------------------------
// Start the Server
// ------------------------------
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await loadData();
  console.log('Server is ready to handle requests');
});
