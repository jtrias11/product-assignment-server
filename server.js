/***************************************************************
 * server.js - Final Script (Updated)
 * 
 * Features:
 * - Loads agents from "Walmart BH Roster.xlsx" (column E).
 * - Loads products from "output.csv" (columns: 
 *   item.abstract_product_id, abstract_product_id, rule_priority, tenant_id, oldest_created_on, count).
 * - Provides endpoints to refresh data, upload output.csv, assign tasks,
 *   complete tasks (including complete all tasks per agent),
 *   unassign tasks, and download CSVs for completed/unassigned items.
 ***************************************************************/

const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs').promises;
const path = require('path');
const csvParser = require('csv-parser');
const xlsx = require('xlsx');
const { createReadStream, createWriteStream } = require('fs');
const multer = require('multer');
const { format } = require('@fast-csv/format'); // for CSV download endpoints

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

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
// Master output CSV (merged file)
const OUTPUT_CSV = path.join(DATA_DIR, 'output.csv');
const ROSTER_EXCEL = path.join(DATA_DIR, 'Walmart BH Roster.xlsx');

// ------------------------------
// Multer Configuration
// ------------------------------
// Save uploaded file with a unique filename so we can merge it later
const storage = multer.diskStorage({
  destination: (req, file, cb) => { cb(null, DATA_DIR); },
  filename: (req, file, cb) => { cb(null, Date.now() + '-' + file.originalname); }
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

// Reads products from output.csv
function readOutputCsv() {
  return new Promise((resolve) => {
    let results = [];
    console.log(`Looking for output CSV at: ${OUTPUT_CSV}`);
    fs.access(OUTPUT_CSV)
      .then(() => {
        createReadStream(OUTPUT_CSV)
          .pipe(csvParser())
          .on('data', (row) => results.push(row))
          .on('end', () => {
            console.log(`Loaded ${results.length} rows from output CSV.`);
            if(results.length > 0) {
              console.log("First row:", results[0]);
            }
            resolve(results);
          })
          .on('error', (error) => {
            console.error('Error reading output CSV:', error);
            resolve([]);
          });
      })
      .catch(() => {
        console.error(`Output CSV file not found at: ${OUTPUT_CSV}`);
        resolve([]);
      });
  });
}

// Reads agents from the Excel roster (using column E)
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
    // Read column E (index 4), ignoring blanks.
    for (let row = 1; row <= range.e.r; row++) {
      const cellRef = xlsx.utils.encode_cell({ r: row, c: 4 });
      const cell = worksheet[cellRef];
      if (cell && cell.v && typeof cell.v === 'string') {
        const name = cell.v.trim();
        if (name !== '' && !['trimmed zoho name', 'name', 'agent name'].includes(name.toLowerCase())) {
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
    // 1. Load agents from JSON (if available) or Excel.
    try {
      const agentsData = await fs.readFile(AGENTS_FILE, 'utf8');
      agents = JSON.parse(agentsData);
      console.log(`Loaded ${agents.length} agents from JSON file`);
    } catch (error) {
      console.log('No agents JSON file found, importing from Excel roster...');
      const excelAgents = await readRosterExcel();
      if (excelAgents.length > 0) {
        agents = excelAgents;
        await saveAgents();
      } else {
        console.log('Excel import failed, using sample agent data');
        agents = [
          { id: 1, name: "Agent Sample 1", role: "Item Review", capacity: 10, currentAssignments: [] },
          { id: 2, name: "Agent Sample 2", role: "Item Review", capacity: 10, currentAssignments: [] }
        ];
        await saveAgents();
      }
    }
    // 2. Load products from output.csv.
    try {
      const csvRows = await readOutputCsv();
      products = csvRows.map(row => ({
        id: row.abstract_product_id || row.item_abstract_product_id || row['item.abstract_product_id'],
        name: "", // update if you have a product name column
        priority: row.rule_priority || row.priority,
        tenantId: row.tenant_id,
        createdOn: row.oldest_created_on || row.sys_created_on || row.created_on,
        count: row.count,
        assigned: false
      }));
      // Filter out any products with null or undefined ID
      products = products.filter(p => p.id);
      console.log(`Loaded ${products.length} products from output CSV`);
    } catch (error) {
      console.log('Error loading products from output CSV:', error);
      products = [];
    }
    // 3. Load assignments from file.
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
  agents.forEach(agent => { agent.currentAssignments = []; });
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
        assignmentId: assignment.id,
        assignedOn: assignment.assignedOn || null,
        completed: assignment.completed || false,
        completedOn: assignment.completedOn || null
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
app.get('/', (req, res) => { res.send('Product Assignment Server is running'); });
app.get('/api/agents', (req, res) => { res.json(agents); });
app.get('/api/products', (req, res) => { res.json(products); });
app.get('/api/assignments', (req, res) => { res.json(assignments); });

// Endpoint to get completed assignments
app.get('/api/completed-assignments', (req, res) => {
  const completed = assignments.filter(a => a.completed);
  res.json(completed);
});

// Endpoint to get unassigned products
app.get('/api/unassigned-products', (req, res) => {
  const unassigned = products.filter(p => !p.assigned);
  res.json(unassigned);
});

// ------------------------------
// File Upload Endpoint (Merge CSV)
// ------------------------------
app.post('/api/upload-output', upload.single('outputFile'), async (req, res) => {
  try {
    console.log('File upload received:', req.file);
    // Read the new CSV data from the uploaded file
    const newData = await new Promise((resolve, reject) => {
      const results = [];
      createReadStream(req.file.path)
        .pipe(csvParser())
        .on('data', (row) => results.push(row))
        .on('end', () => resolve(results))
        .on('error', reject);
    });

    // Read existing CSV data if the master output CSV exists
    let existingData = [];
    if (await fileExists(OUTPUT_CSV)) {
      existingData = await new Promise((resolve, reject) => {
        const results = [];
        createReadStream(OUTPUT_CSV)
          .pipe(csvParser())
          .on('data', (row) => results.push(row))
          .on('end', () => resolve(results))
          .on('error', reject);
      });
    }

    // Merge newData with existingData based on a unique product identifier.
    // Adjust the key field as needed (e.g., abstract_product_id)
    const mergedDataMap = new Map();
    // Add existing data
    existingData.forEach(row => {
      const key = row.abstract_product_id || row.item_abstract_product_id || row['item.abstract_product_id'];
      if (key) mergedDataMap.set(key, row);
    });
    // Merge in new data (update or add new rows)
    newData.forEach(row => {
      const key = row.abstract_product_id || row.item_abstract_product_id || row['item.abstract_product_id'];
      if (key) {
        mergedDataMap.set(key, row);
      }
    });
    const mergedData = Array.from(mergedDataMap.values());

    // Write merged data back to the master CSV file (OUTPUT_CSV)
    const ws = createWriteStream(OUTPUT_CSV);
    const csvStream = format({ headers: true });
    csvStream.pipe(ws);
    mergedData.forEach(row => csvStream.write(row));
    csvStream.end();

    // Optionally remove the temporary uploaded file
    await fs.unlink(req.file.path);
    
    // Reload data (this will now include the merged products)
    await loadData();
    res.status(200).json({ message: 'Output CSV uploaded and merged successfully. Data refreshed.' });
  } catch (error) {
    console.error('Error uploading output CSV:', error);
    res.status(500).json({ error: error.message });
  }
});

// Refresh endpoint: re-read data from output CSV and roster Excel.
app.post('/api/refresh', async (req, res) => {
  try {
    await loadData();
    console.log('Data refreshed successfully from output CSV and roster Excel');
    res.status(200).json({ message: 'Data refreshed successfully' });
  } catch (error) {
    console.error('Error refreshing data:', error);
    res.status(500).json({ error: 'Failed to refresh data' });
  }
});

// Assign endpoint: assign the oldest unassigned product to an agent.
let assignmentInProgress = false;
app.post('/api/assign', async (req, res) => {
  if (assignmentInProgress) {
    return res.status(409).json({ error: 'Another assignment is in progress, please try again later' });
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
    assignments.forEach(a => assignedProductIds.add(a.productId));
    const availableProducts = products
      .filter(p => !p.assigned && !assignedProductIds.has(p.id))
      .sort((a, b) => new Date(a.createdOn) - new Date(b.createdOn));
    if (availableProducts.length === 0) {
      assignmentInProgress = false;
      return res.status(404).json({ error: 'No available products to assign' });
    }
    const productToAssign = availableProducts[0];
    productToAssign.assigned = true;
    const newAssignment = {
      id: uuidv4(),
      agentId: agent.id,
      productId: productToAssign.id,
      assignedOn: new Date().toISOString().replace('T', ' ').substring(0, 19),
      completed: false,
      completedOn: null
    };
    assignments.push(newAssignment);
    await saveAssignments();
    updateAgentAssignments();
    await saveAgents();
    assignmentInProgress = false;
    res.status(200).json({
      message: `Task ${productToAssign.id} assigned to ${agent.name}`,
      assignment: newAssignment
    });
  } catch (error) {
    assignmentInProgress = false;
    console.error('Error assigning task:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Complete endpoint: mark an assignment as completed.
app.post('/api/complete', async (req, res) => {
  try {
    const { agentId, productId } = req.body;
    if (!agentId || !productId) {
      return res.status(400).json({ error: 'agentId and productId are required' });
    }
    const agent = agents.find(a => a.id === agentId);
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }
    const assignmentIndex = assignments.findIndex(a =>
      a.agentId === agentId && a.productId === productId && !a.completed
    );
    if (assignmentIndex === -1) {
      return res.status(404).json({ error: 'Active assignment not found' });
    }
    assignments[assignmentIndex].completed = true;
    assignments[assignmentIndex].completedOn = new Date().toISOString().replace('T', ' ').substring(0, 19);
    const product = products.find(p => p.id === productId);
    if (product) {
      product.assigned = false;
    }
    agent.currentAssignments = agent.currentAssignments.filter(task => task.productId !== productId);
    await saveAssignments();
    updateAgentAssignments();
    await saveAgents();
    res.status(200).json({ message: `Task ${productId} completed by ${agent.name}` });
  } catch (error) {
    console.error('Error completing task:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// ------------------------------
// New Endpoint: Complete All Tasks for Agent
// ------------------------------
app.post('/api/complete-all-agent', async (req, res) => {
  try {
    const { agentId } = req.body;
    if (!agentId) {
      return res.status(400).json({ error: 'Agent ID is required' });
    }
    const agent = agents.find(a => a.id === agentId);
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }
    const activeAssignments = assignments.filter(a => a.agentId === agentId && !a.completed);
    if (activeAssignments.length === 0) {
      return res.status(200).json({ message: 'No active tasks to complete for this agent' });
    }
    activeAssignments.forEach(assignment => {
      assignment.completed = true;
      assignment.completedOn = new Date().toISOString().replace('T', ' ').substring(0, 19);
      const product = products.find(p => p.id === assignment.productId);
      if (product) {
        product.assigned = false;
      }
    });
    agent.currentAssignments = [];
    await saveAssignments();
    updateAgentAssignments();
    await saveAgents();
    res.status(200).json({ message: `Completed all (${activeAssignments.length}) tasks for agent ${agent.name}` });
  } catch (error) {
    console.error('Error completing all tasks for agent:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Unassign a single product.
app.post('/api/unassign-product', async (req, res) => {
  try {
    const { productId } = req.body;
    if (!productId) {
      return res.status(400).json({ error: 'Product ID is required' });
    }
    const productAssignments = assignments.filter(a => a.productId === productId && !a.completed);
    if (productAssignments.length === 0) {
      return res.status(404).json({ error: 'No active assignment found for this product' });
    }
    productAssignments.forEach(assignment => {
      const agent = agents.find(a => a.id === assignment.agentId);
      if (agent) {
        agent.currentAssignments = agent.currentAssignments.filter(task => task.productId !== productId);
      }
    });
    assignments = assignments.filter(a => a.productId !== productId || a.completed);
    const product = products.find(p => p.id === productId);
    if (product) {
      product.assigned = false;
    }
    await saveAssignments();
    updateAgentAssignments();
    await saveAgents();
    res.status(200).json({ message: `Product ${productId} unassigned successfully` });
  } catch (error) {
    console.error('Error unassigning product:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Unassign all tasks from a specific agent.
app.post('/api/unassign-agent', async (req, res) => {
  try {
    const { agentId } = req.body;
    if (!agentId) {
      return res.status(400).json({ error: 'Agent ID is required' });
    }
    const agent = agents.find(a => a.id === agentId);
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }
    const tasksCount = agent.currentAssignments.length;
    if (tasksCount === 0) {
      return res.status(200).json({ message: 'Agent has no tasks to unassign' });
    }
    agent.currentAssignments.forEach(task => {
      const product = products.find(p => p.id === task.productId);
      if (product) {
        product.assigned = false;
      }
    });
    assignments = assignments.filter(a => a.agentId !== agentId || a.completed);
    agent.currentAssignments = [];
    await saveAssignments();
    updateAgentAssignments();
    await saveAgents();
    res.status(200).json({ message: `Unassigned ${tasksCount} tasks from agent ${agent.name}` });
  } catch (error) {
    console.error('Error unassigning agent tasks:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Unassign all tasks from all agents.
app.post('/api/unassign-all', async (req, res) => {
  try {
    const totalActive = assignments.filter(a => !a.completed).length;
    assignments = assignments.filter(a => a.completed);
    products.forEach(p => { p.assigned = false; });
    agents.forEach(a => { a.currentAssignments = []; });
    await saveAssignments();
    await saveAgents();
    res.status(200).json({ message: `Unassigned ${totalActive} tasks from all agents` });
  } catch (error) {
    console.error('Error unassigning all tasks:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Download completed assignments as CSV.
app.get('/api/download/completed-assignments', (req, res) => {
  const completed = assignments.filter(a => a.completed);
  res.setHeader('Content-disposition', 'attachment; filename=completed-tasks.csv');
  res.setHeader('Content-Type', 'text/csv');
  const csvStream = format({ headers: true });
  csvStream.pipe(res);
  completed.forEach(a => {
    const agent = agents.find(ag => ag.id === a.agentId);
    csvStream.write({
      assignmentId: a.id,
      agentName: agent ? agent.name : 'Unknown',
      productId: a.productId,
      assignedOn: a.assignedOn,
      completedOn: a.completedOn
    });
  });
  csvStream.end();
});

// Download unassigned products as CSV.
app.get('/api/download/unassigned-products', (req, res) => {
  const unassigned = products.filter(p => !p.assigned);
  res.setHeader('Content-disposition', 'attachment; filename=unassigned-products.csv');
  res.setHeader('Content-Type', 'text/csv');
  const csvStream = format({ headers: true });
  csvStream.pipe(res);
  unassigned.forEach(p => {
    csvStream.write({
      productId: p.id,
      priority: p.priority,
      tenantId: p.tenantId,
      createdOn: p.createdOn,
      count: p.count
    });
  });
  csvStream.end();
});

// Download previously assigned products as CSV.
app.get('/api/download/previously-assigned', (req, res) => {
  // Previously assigned products are those that were assigned and then unassigned.
  // For this example, we'll consider assignments that are completed.
  const previouslyAssigned = assignments.filter(a => a.completed);
  res.setHeader('Content-disposition', 'attachment; filename=previously-assigned.csv');
  res.setHeader('Content-Type', 'text/csv');
  const csvStream = format({ headers: true });
  csvStream.pipe(res);
  previouslyAssigned.forEach(a => {
    const agent = agents.find(ag => ag.id === a.agentId);
    csvStream.write({
      assignmentId: a.id,
      agentName: agent ? agent.name : 'Unknown',
      productId: a.productId,
      assignedOn: a.assignedOn,
      completedOn: a.completedOn
    });
  });
  csvStream.end();
});

// Download complete product queue as CSV.
app.get('/api/download/queue', (req, res) => {
  // For the product queue, we'll consider all products.
  res.setHeader('Content-disposition', 'attachment; filename=product-queue.csv');
  res.setHeader('Content-Type', 'text/csv');
  const csvStream = format({ headers: true });
  csvStream.pipe(res);
  products.forEach(p => {
    csvStream.write({
      productId: p.id,
      priority: p.priority,
      tenantId: p.tenantId,
      createdOn: p.createdOn,
      count: p.count,
      assigned: p.assigned ? "Yes" : "No"
    });
  });
  csvStream.end();
});

// ------------------------------
// Start the Server
// ------------------------------
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await loadData();
  console.log('Server is ready to handle requests');
});
