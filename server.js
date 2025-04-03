/***************************************************************
 * server.js - Final Script with Optimized CSV Upload and 6-Column Mapping
 * 
 * Expected CSV Columns:
 *   1) item.abstract_product_id
 *   2) abstract_product_id
 *   3) rule_priority
 *   4) tenant_id
 *   5) oldest_created_on
 *   6) count
 *
 * Features:
 * - Connects to MongoDB via MONGO_URI.
 * - Loads agents from "Walmart BH Roster.xlsx" (using column E) if none exist.
 * - Loads products from "output.csv" if none exist.
 * - CSV upload endpoint uses bulkWrite for efficient updates.
 * - Provides endpoints for refreshing data, task assignment,
 *   task completion, unassignment, and CSV downloads.
 ***************************************************************/

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs').promises;
const path = require('path');
const csvParser = require('csv-parser');
const xlsx = require('xlsx');
const { createReadStream } = require('fs');
const multer = require('multer');
const { format } = require('@fast-csv/format');
const mongoose = require('mongoose');

// Import Models
const Agent = require('./models/Agent');
const Product = require('./models/Product');
const Assignment = require('./models/Assignment');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

// ------------------------------
// MongoDB Connection
// ------------------------------
mongoose.connect(process.env.MONGO_URI)
  .then(() => console.log('MongoDB Connected'))
  .catch((error) => console.error('MongoDB Connection Error:', error));

// ------------------------------
// File Paths and Directories
// ------------------------------
const DATA_DIR = path.join(__dirname, 'data');
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

// Reads the entire CSV from OUTPUT_CSV
async function readInitialCsv() {
  console.log(`Looking for output CSV at: ${OUTPUT_CSV}`);
  if (!(await fileExists(OUTPUT_CSV))) {
    console.error(`No output.csv found at: ${OUTPUT_CSV}`);
    return [];
  }
  const rows = [];
  return new Promise((resolve, reject) => {
    createReadStream(OUTPUT_CSV)
      .pipe(csvParser())
      .on('data', row => rows.push(row))
      .on('end', () => {
        console.log(`Loaded ${rows.length} rows from output CSV.`);
        resolve(rows);
      })
      .on('error', (error) => {
        console.error('Error reading output CSV:', error);
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
            name,
            role: "Item Review",
            capacity: 30,
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
// Load Data into MongoDB on Startup
// ------------------------------
async function loadData() {
  await ensureDataDir();

  // Agents: Import from Excel if none exist.
  const agentCount = await Agent.countDocuments();
  if (agentCount === 0) {
    console.log('No agents found in MongoDB, importing from Excel roster...');
    const excelAgents = await readRosterExcel();
    if (excelAgents.length > 0) {
      await Agent.insertMany(excelAgents);
      console.log(`Imported ${excelAgents.length} agents from Excel roster`);
    } else {
      const sampleAgents = [
        { name: "Agent Sample 1", role: "Item Review", capacity: 30 },
        { name: "Agent Sample 2", role: "Item Review", capacity: 30 }
      ];
      await Agent.insertMany(sampleAgents);
      console.log('Inserted sample agents');
    }
  }

  // Products: Import from CSV if none exist.
  const productCount = await Product.countDocuments();
  if (productCount === 0) {
    console.log('No products found in MongoDB, importing from CSV...');
    const csvRows = await readInitialCsv();
    const csvProducts = csvRows.map(row => {
      const productId = row['abstract_product_id'] || row['item.abstract_product_id'];
      return {
        id: productId,
        name: productId || "Unnamed Product",
        priority: row['rule_priority'] || null,
        tenantId: row['tenant_id'] || null,
        createdOn: row['oldest_created_on'] || null,
        count: row['count'] || 1,
        assigned: false
      };
    }).filter(p => p.id);
    if (csvProducts.length > 0) {
      await Product.insertMany(csvProducts);
      console.log(`Imported ${csvProducts.length} products from CSV`);
    }
  }
  console.log('Data load complete');
}

// ------------------------------
// API Routes
// ------------------------------
app.get('/', (req, res) => {
  res.send('Product Assignment Server is running');
});

app.get('/api/agents', async (req, res) => {
  try {
    const agents = await Agent.find();
    res.json(agents);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/products', async (req, res) => {
  try {
    const products = await Product.find();
    res.json(products);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/assignments', async (req, res) => {
  try {
    const assignments = await Assignment.find();
    res.json(assignments);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Completed assignments
app.get('/api/completed-assignments', async (req, res) => {
  try {
    const completed = await Assignment.find({ completed: true });
    res.json(completed);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Unassigned products
app.get('/api/unassigned-products', async (req, res) => {
  try {
    const unassigned = await Product.find({ assigned: false });
    res.json(unassigned);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Previously assigned (completed or unassigned)
app.get('/api/previously-assigned', async (req, res) => {
  try {
    const prev = await Assignment.find({
      $or: [{ completed: true }, { unassignedTime: { $exists: true } }]
    });
    const result = [];
    for (const a of prev) {
      const product = await Product.findOne({ id: a.productId });
      result.push({
        id: product ? product.id : a.productId,
        count: product ? product.count : '',
        tenantId: product ? product.tenantId : '',
        priority: product ? product.priority : '',
        createdOn: product ? product.createdOn : '',
        unassignedTime: a.unassignedTime || '',
        unassignedBy: a.unassignedBy || ''
      });
    }
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Queue: all products
app.get('/api/queue', async (req, res) => {
  try {
    const products = await Product.find();
    res.json(products);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ------------------------------
// Optimized CSV Upload Endpoint (Using bulkWrite)
// ------------------------------
app.post('/api/upload-output', upload.single('outputFile'), async (req, res) => {
  try {
    console.log('CSV file upload received:', req.file.path);
    const rows = [];
    await new Promise((resolve, reject) => {
      createReadStream(req.file.path)
        .pipe(csvParser())
        .on('data', row => {
          console.log('Row:', row); // Debug: check keys and values
          rows.push(row);
        })
        .on('end', resolve)
        .on('error', reject);
    });

    const bulkOps = [];
    for (const row of rows) {
      const productId = row['abstract_product_id'] || row['item.abstract_product_id'];
      if (!productId) continue;
      bulkOps.push({
        updateOne: {
          filter: { id: productId },
          update: {
            $set: {
              id: productId,
              name: productId, // Derive name from productId
              priority: row['rule_priority'] || null,
              tenantId: row['tenant_id'] || null,
              createdOn: row['oldest_created_on'] || null,
              count: row['count'] || 1,
              assigned: false  // Force unassigned on upload
            }
          },
          upsert: true
        }
      });
    }
    if (bulkOps.length > 0) {
      await Product.bulkWrite(bulkOps);
      console.log(`BulkWrite processed ${bulkOps.length} product updates`);
    }
    await fs.unlink(req.file.path);
    res.status(200).json({ message: 'CSV uploaded and products updated successfully' });
  } catch (error) {
    console.error('Error uploading CSV:', error);
    res.status(500).json({ error: error.message });
  }
});

// Refresh endpoint: reload data from CSV/Excel if needed
app.post('/api/refresh', async (req, res) => {
  try {
    await loadData();
    res.status(200).json({ message: 'Data refreshed successfully' });
  } catch (error) {
    res.status(500).json({ error: 'Failed to refresh data' });
  }
});

// ------------------------------
// Task Assignment and Completion Endpoints
// ------------------------------
let assignmentInProgress = false;

// Assign a product to an agent
app.post('/api/assign', async (req, res) => {
  if (assignmentInProgress) {
    return res.status(409).json({ error: 'Another assignment is in progress' });
  }
  assignmentInProgress = true;
  try {
    const { agentId } = req.body;
    if (!agentId) {
      assignmentInProgress = false;
      return res.status(400).json({ error: 'Agent ID is required' });
    }
    const agent = await Agent.findById(agentId);
    if (!agent) {
      assignmentInProgress = false;
      return res.status(404).json({ error: 'Agent not found' });
    }
    const activeCount = await Assignment.countDocuments({
      agentId: agent._id,
      completed: false,
      unassignedTime: { $exists: false }
    });
    if (activeCount >= agent.capacity) {
      assignmentInProgress = false;
      return res.status(400).json({ error: 'Agent has reached maximum capacity' });
    }
    const assignedProductIds = (await Assignment.find({})).map(a => a.productId);
    const availableProduct = await Product.findOne({
      assigned: false,
      id: { $nin: assignedProductIds }
    }).sort({ createdOn: 1 });
    if (!availableProduct) {
      assignmentInProgress = false;
      return res.status(404).json({ error: 'No available products to assign' });
    }
    availableProduct.assigned = true;
    await availableProduct.save();
    const newAssignment = await Assignment.create({
      agentId: agent._id,
      productId: availableProduct.id,
      assignedOn: new Date().toISOString().replace('T', ' ').substring(0, 19),
      completed: false
    });
    assignmentInProgress = false;
    res.status(200).json({
      message: `Task ${availableProduct.id} assigned to ${agent.name}`,
      assignment: newAssignment
    });
  } catch (error) {
    assignmentInProgress = false;
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Complete a single task
app.post('/api/complete', async (req, res) => {
  try {
    const { agentId, productId } = req.body;
    if (!agentId || !productId) {
      return res.status(400).json({ error: 'agentId and productId are required' });
    }
    const agent = await Agent.findById(agentId);
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }
    const assignment = await Assignment.findOne({
      agentId: agent._id,
      productId,
      completed: false,
      unassignedTime: { $exists: false }
    });
    if (!assignment) {
      return res.status(404).json({ error: 'Active assignment not found' });
    }
    assignment.completed = true;
    assignment.completedOn = new Date().toISOString().replace('T', ' ').substring(0, 19);
    await assignment.save();
    const product = await Product.findOne({ id: productId });
    if (product) {
      product.assigned = false;
      await product.save();
    }
    res.status(200).json({ message: `Task ${productId} completed by ${agent.name}` });
  } catch (error) {
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Complete all tasks for an agent
app.post('/api/complete-all-agent', async (req, res) => {
  try {
    const { agentId } = req.body;
    if (!agentId) {
      return res.status(400).json({ error: 'Agent ID is required' });
    }
    const agent = await Agent.findById(agentId);
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }
    const activeAssignments = await Assignment.find({
      agentId: agent._id,
      completed: false,
      unassignedTime: { $exists: false }
    });
    if (activeAssignments.length === 0) {
      return res.status(200).json({ message: 'No active tasks for this agent' });
    }
    for (const assignment of activeAssignments) {
      assignment.completed = true;
      assignment.completedOn = new Date().toISOString().replace('T', ' ').substring(0, 19);
      await assignment.save();
      const product = await Product.findOne({ id: assignment.productId });
      if (product) {
        product.assigned = false;
        await product.save();
      }
    }
    res.status(200).json({
      message: `Completed all (${activeAssignments.length}) tasks for agent ${agent.name}`
    });
  } catch (error) {
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Unassign a single product
app.post('/api/unassign-product', async (req, res) => {
  try {
    const { productId } = req.body;
    if (!productId) {
      return res.status(400).json({ error: 'Product ID is required' });
    }
    const productAssignments = await Assignment.find({
      productId,
      completed: false,
      unassignedTime: { $exists: false }
    });
    if (productAssignments.length === 0) {
      return res.status(404).json({ error: 'No active assignment found for this product' });
    }
    for (const assignment of productAssignments) {
      const agent = await Agent.findById(assignment.agentId);
      assignment.unassignedTime = new Date().toISOString().replace('T', ' ').substring(0, 19);
      assignment.unassignedBy = agent ? agent.name : 'Unknown';
      await assignment.save();
    }
    const product = await Product.findOne({ id: productId });
    if (product) {
      product.assigned = false;
      await product.save();
    }
    res.status(200).json({ message: `Product ${productId} unassigned successfully` });
  } catch (error) {
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Unassign all tasks from a specific agent
app.post('/api/unassign-agent', async (req, res) => {
  try {
    const { agentId } = req.body;
    if (!agentId) {
      return res.status(400).json({ error: 'Agent ID is required' });
    }
    const agent = await Agent.findById(agentId);
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }
    const activeAssignments = await Assignment.find({
      agentId: agent._id,
      completed: false,
      unassignedTime: { $exists: false }
    });
    if (activeAssignments.length === 0) {
      return res.status(200).json({ message: 'Agent has no tasks to unassign' });
    }
    for (const assignment of activeAssignments) {
      assignment.unassignedTime = new Date().toISOString().replace('T', ' ').substring(0, 19);
      assignment.unassignedBy = agent.name;
      await assignment.save();
      const product = await Product.findOne({ id: assignment.productId });
      if (product) {
        product.assigned = false;
        await product.save();
      }
    }
    res.status(200).json({
      message: `Unassigned ${activeAssignments.length} tasks from agent ${agent.name}`
    });
  } catch (error) {
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Unassign all tasks from all agents
app.post('/api/unassign-all', async (req, res) => {
  try {
    const activeAssignments = await Assignment.find({
      completed: false,
      unassignedTime: { $exists: false }
    });
    for (const assignment of activeAssignments) {
      assignment.unassignedTime = new Date().toISOString().replace('T', ' ').substring(0, 19);
      const agent = await Agent.findById(assignment.agentId);
      assignment.unassignedBy = agent ? agent.name : 'Unknown';
      await assignment.save();
      const product = await Product.findOne({ id: assignment.productId });
      if (product) {
        product.assigned = false;
        await product.save();
      }
    }
    res.status(200).json({
      message: `Unassigned ${activeAssignments.length} tasks from all agents`
    });
  } catch (error) {
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// ------------------------------
// CSV Download Endpoints
// ------------------------------
app.get('/api/download/completed-assignments', async (req, res) => {
  try {
    const completed = await Assignment.find({ completed: true });
    res.setHeader('Content-disposition', 'attachment; filename=completed-tasks.csv');
    res.setHeader('Content-Type', 'text/csv');
    const csvStream = format({ headers: true });
    csvStream.pipe(res);
    for (const a of completed) {
      const agent = await Agent.findById(a.agentId);
      csvStream.write({
        assignmentId: a._id,
        agentId: a.agentId,
        completedBy: agent ? agent.name : 'Unknown',
        productId: a.productId,
        assignedOn: a.assignedOn,
        completedOn: a.completedOn
      });
    }
    csvStream.end();
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/download/unassigned-products', async (req, res) => {
  try {
    const unassigned = await Product.find({ assigned: false });
    res.setHeader('Content-disposition', 'attachment; filename=unassigned-products.csv');
    res.setHeader('Content-Type', 'text/csv');
    const csvStream = format({ headers: true });
    csvStream.pipe(res);
    for (const p of unassigned) {
      csvStream.write({
        productId: p.id,
        priority: p.priority,
        tenantId: p.tenantId,
        createdOn: p.createdOn,
        count: p.count
      });
    }
    csvStream.end();
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/download/previously-assigned', async (req, res) => {
  try {
    const prev = await Assignment.find({
      $or: [{ completed: true }, { unassignedTime: { $exists: true } }]
    });
    const results = [];
    for (const a of prev) {
      const product = await Product.findOne({ id: a.productId });
      results.push({
        productId: product ? product.id : a.productId,
        count: product ? product.count : '',
        tenantId: product ? product.tenantId : '',
        priority: product ? product.priority : '',
        createdOn: product ? product.createdOn : '',
        unassignedTime: a.unassignedTime || '',
        unassignedBy: a.unassignedBy || ''
      });
    }
    res.setHeader('Content-disposition', 'attachment; filename=previously-assigned.csv');
    res.setHeader('Content-Type', 'text/csv');
    const csvStream = format({ headers: true });
    csvStream.pipe(res);
    for (const row of results) {
      csvStream.write(row);
    }
    csvStream.end();
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/download/queue', async (req, res) => {
  try {
    const allProducts = await Product.find();
    res.setHeader('Content-disposition', 'attachment; filename=product-queue.csv');
    res.setHeader('Content-Type', 'text/csv');
    const csvStream = format({ headers: true });
    csvStream.pipe(res);
    for (const p of allProducts) {
      csvStream.write({
        productId: p.id,
        priority: p.priority,
        tenantId: p.tenantId,
        createdOn: p.createdOn,
        count: p.count,
        assigned: p.assigned ? "Yes" : "No"
      });
    }
    csvStream.end();
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ------------------------------
// Start the Server
// ------------------------------
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await loadData();
  console.log('Server is ready to handle requests');
});
