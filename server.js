/***************************************************************
 * server.js - Final Script (Updated)
 * 
 * Features:
 * - Loads agents from "Walmart BH Roster.xlsx" (column E).
 * - Loads products from "output.csv" (using abstract_product_id as primary).
 * - Provides endpoints to refresh data, upload CSV (merging data), assign tasks,
 *   complete tasks (including complete all tasks per agent),
 *   unassign tasks (which now marks assignments as unassigned rather than removing them),
 *   and download CSVs for completed, unassigned, and full queue.
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
const { format } = require('@fast-csv/format');

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
const OUTPUT_CSV = path.join(DATA_DIR, 'output.csv');
const ROSTER_EXCEL = path.join(DATA_DIR, 'Walmart BH Roster.xlsx');

// ------------------------------
// Multer Configuration
// ------------------------------
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
      const possibleSheetNames = ["Agents", "AgentsList", "Agents_List", "Agent List", "Agent_List"];
      for (const name of possibleSheetNames) {
        if (workbook.SheetNames.includes(name)) {
          sheetName = name;
          break;
        }
      }
      if (!workbook.SheetNames.includes(sheetName)) {
        sheetName = workbook.SheetNames[0];
      }
    }
    const worksheet = workbook.Sheets[sheetName];
    const range = xlsx.utils.decode_range(worksheet['!ref']);
    const agentsList = [];
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
    // Load agents
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
        agents = [
          { id: 1, name: "Agent Sample 1", role: "Item Review", capacity: 10, currentAssignments: [] },
          { id: 2, name: "Agent Sample 2", role: "Item Review", capacity: 10, currentAssignments: [] }
        ];
        await saveAgents();
      }
    }
    // Load products from CSV
    try {
      const csvRows = await readOutputCsv();
      products = csvRows.map(row => ({
        id: row.abstract_product_id || row.item_abstract_product_id || row['item.abstract_product_id'] || row.product_id,
        name: row.product_name || "",
        priority: row.rule_priority || row.priority,
        tenantId: row.tenant_id,
        createdOn: row.oldest_created_on || row.sys_created_on || row.created_on,
        count: row.count,
        assigned: false
      }));
      products = products.filter(p => p.id);
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
  agents.forEach(agent => { agent.currentAssignments = []; });
  assignments.forEach(assignment => {
    if (!assignment.completed && !assignment.unassignedTime) {
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

// Completed assignments endpoint
app.get('/api/completed-assignments', (req, res) => {
  const completed = assignments.filter(a => a.completed);
  res.json(completed);
});

// Unassigned products endpoint
app.get('/api/unassigned-products', (req, res) => {
  const unassigned = products.filter(p => !p.assigned);
  res.json(unassigned);
});

// Previously assigned (unassigned or completed) endpoint
app.get('/api/previously-assigned', (req, res) => {
  // Map assignments to include correct product data
  const prev = assignments.filter(a => a.completed || a.unassignedTime).map(a => {
    const product = products.find(p => p.id === a.productId);
    return {
      id: product ? product.id : a.productId,
      count: product ? product.count : '',
      tenantId: product ? product.tenantId : '',
      priority: product ? product.priority : '',
      createdOn: product ? product.createdOn : '',
      unassignedTime: a.unassignedTime || '',
      unassignedBy: a.unassignedBy || ''
    };
  });
  res.json(prev);
});

// Queue endpoint: return all products
app.get('/api/queue', (req, res) => {
  res.json(products);
});

// File Upload Endpoint (Merge CSV)
app.post('/api/upload-output', upload.single('outputFile'), async (req, res) => {
  try {
    console.log('File upload received:', req.file);
    const newData = await new Promise((resolve, reject) => {
      const results = [];
      createReadStream(req.file.path)
        .pipe(csvParser())
        .on('data', (row) => results.push(row))
        .on('end', () => resolve(results))
        .on('error', reject);
    });
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
    const mergedDataMap = new Map();
    existingData.forEach(row => {
      const key = row.abstract_product_id || row.item_abstract_product_id || row['item.abstract_product_id'] || row.product_id;
      if (key) mergedDataMap.set(key, row);
    });
    newData.forEach(row => {
      const key = row.abstract_product_id || row.item_abstract_product_id || row['item.abstract_product_id'] || row.product_id;
      if (key) {
        mergedDataMap.set(key, row);
      }
    });
    const mergedData = Array.from(mergedDataMap.values());
    const ws = createWriteStream(OUTPUT_CSV);
    const csvStream = format({ headers: true });
    csvStream.pipe(ws);
    mergedData.forEach(row => csvStream.write(row));
    csvStream.end();
    await fs.unlink(req.file.path);
    await loadData();
    res.status(200).json({ message: 'Output CSV uploaded and merged successfully. Data refreshed.' });
  } catch (error) {
    console.error('Error uploading output CSV:', error);
    res.status(500).json({ error: error.message });
  }
});

// Refresh endpoint
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

// Assign endpoint
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

// Complete endpoint
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
      a.agentId === agentId && a.productId === productId && !a.completed && !a.unassignedTime
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
    await saveAssignments();
    updateAgentAssignments();
    await saveAgents();
    res.status(200).json({ message: `Task ${productId} completed by ${agent.name}` });
  } catch (error) {
    console.error('Error completing task:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Complete All Tasks endpoint
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
    const activeAssignments = assignments.filter(a => a.agentId === agentId && !a.completed && !a.unassignedTime);
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
    const { productId, agentId } = req.body;
    if (!productId) {
      return res.status(400).json({ error: 'Product ID is required' });
    }
    const productAssignments = assignments.filter(a => a.productId === productId && !a.completed && !a.unassignedTime);
    if (productAssignments.length === 0) {
      return res.status(404).json({ error: 'No active assignment found for this product' });
    }
    productAssignments.forEach(assignment => {
      const agent = agents.find(a => a.id === assignment.agentId);
      if (agent) {
        agent.currentAssignments = agent.currentAssignments.filter(task => task.productId !== productId);
      }
      assignment.unassignedTime = new Date().toISOString().replace('T', ' ').substring(0, 19);
      assignment.unassignedBy = agent ? agent.name : 'Unknown';
      assignment.wasUnassigned = true;
    });
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
    assignments.forEach(a => {
      if (a.agentId === agentId && !a.completed && !a.unassignedTime) {
        a.unassignedTime = new Date().toISOString().replace('T', ' ').substring(0, 19);
        const agentFound = agents.find(ag => ag.id === a.agentId);
        a.unassignedBy = agentFound ? agentFound.name : 'Unknown';
        a.wasUnassigned = true;
      }
    });
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
    const totalActive = assignments.filter(a => !a.completed && !a.unassignedTime).length;
    assignments.forEach(a => {
      if (!a.completed && !a.unassignedTime) {
        a.unassignedTime = new Date().toISOString().replace('T', ' ').substring(0, 19);
        const agent = agents.find(ag => ag.id === a.agentId);
        a.unassignedBy = agent ? agent.name : 'Unknown';
        a.wasUnassigned = true;
      }
    });
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

// Download endpoints
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
      agentId: a.agentId,
      completedBy: agent ? agent.name : 'Unknown',
      productId: a.productId,
      assignedOn: a.assignedOn,
      completedOn: a.completedOn
    });
  });
  csvStream.end();
});

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

app.get('/api/download/previously-assigned', (req, res) => {
  const prev = assignments.filter(a => a.completed || a.unassignedTime).map(a => {
    const product = products.find(p => p.id === a.productId);
    return {
      productId: product ? product.id : a.productId,
      count: product ? product.count : '',
      tenantId: product ? product.tenantId : '',
      priority: product ? product.priority : '',
      createdOn: product ? product.createdOn : '',
      unassignedTime: a.unassignedTime || '',
      unassignedBy: a.unassignedBy || ''
    };
  });
  res.setHeader('Content-disposition', 'attachment; filename=previously-assigned.csv');
  res.setHeader('Content-Type', 'text/csv');
  const csvStream = format({ headers: true });
  csvStream.pipe(res);
  prev.forEach(row => csvStream.write(row));
  csvStream.end();
});

app.get('/api/download/queue', (req, res) => {
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

// Start the server
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await loadData();
  console.log('Server is ready to handle requests');
});
