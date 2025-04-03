/***************************************************************
 * server.js - Final Script with Optimized CSV Upload and Task Query
 * 
 * Expected CSV Columns (header row):
 *   item.abstract_product_id, abstract_product_id, rule_priority, tenant_id, oldest_created_on, count
 * 
 * Features:
 * - Connects to MongoDB via MONGO_URI.
 * - Loads agents from "Walmart BH Roster.xlsx" (using column E) if none exist.
 * - Loads products from "output.csv" if none exist.
 * - CSV upload endpoint uses bulkWrite to efficiently update/insert products.
 * - The /api/assign endpoint now queries directly for an available product.
 * - Provides endpoints for refreshing data, task assignment, completion, unassignment, and CSV downloads.
 ***************************************************************/

require('dotenv').config();
const express = require('express');
const cors = require('cors');
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

// Performance monitoring middleware
app.use((req, res, next) => {
  const start = Date.now();
  
  res.on('finish', () => {
    const duration = Date.now() - start;
    console.log(`${req.method} ${req.url} - ${duration}ms`);
    
    if (duration > 1000) {
      console.warn(`Slow request: ${req.method} ${req.url} - ${duration}ms`);
    }
  });
  
  next();
});

// ------------------------------
// MongoDB Connection (options removed to silence warnings)
mongoose.connect(process.env.MONGO_URI)
  .then(() => console.log('MongoDB Connected'))
  .catch((error) => console.error('MongoDB Connection Error:', error));

// ------------------------------
// File Paths and Directories
const DATA_DIR = path.join(__dirname, 'data');
const OUTPUT_CSV = path.join(DATA_DIR, 'output.csv');
const ROSTER_EXCEL = path.join(DATA_DIR, 'Walmart BH Roster.xlsx');

// ------------------------------
// Multer Configuration
const storage = multer.diskStorage({
  destination: (req, file, cb) => { cb(null, DATA_DIR); },
  filename: (req, file, cb) => { cb(null, Date.now() + '-' + file.originalname); }
});
const upload = multer({ storage });

// ------------------------------
// Helper Functions
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

// Create indexes for faster queries
async function createIndexes() {
  try {
    // Create indexes for faster queries
    await Product.collection.createIndex({ assigned: 1, createdOn: 1 });
    await Assignment.collection.createIndex({ agentId: 1, completed: 1, unassignedTime: 1 });
    await Assignment.collection.createIndex({ productId: 1 });
    await Assignment.collection.createIndex({ 
      completed: 1, 
      unassignedTime: 1 
    });
    console.log('Database indexes created/updated');
  } catch (error) {
    console.error('Error creating indexes:', error);
  }
}

// Simple in-memory cache implementation
const cache = {
  data: {},
  timestamps: {},
  ttl: 60000, // 1 minute default TTL
  
  get(key) {
    const now = Date.now();
    if (this.timestamps[key] && now - this.timestamps[key] < this.ttl) {
      return this.data[key];
    }
    return null;
  },
  
  set(key, value, customTtl) {
    this.data[key] = value;
    this.timestamps[key] = Date.now();
    if (customTtl) this.ttl = customTtl;
  },
  
  invalidate(key) {
    if (key) {
      delete this.data[key];
      delete this.timestamps[key];
    } else {
      // Invalidate all cache if no key provided
      this.data = {};
      this.timestamps = {};
    }
  }
};

// Reads the CSV from OUTPUT_CSV for initial import
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

// Reads agents from Excel roster (using column E)
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
async function loadData() {
  await ensureDataDir();

  // Create database indexes for better performance
  await createIndexes();

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
        count: Number(row['count']) || 1,
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
    const agents = await Agent.find({}, {
      name: 1,
      role: 1,
      capacity: 1
    }).lean();
    res.json(agents);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/products', async (req, res) => {
  try {
    const products = await Product.find({}, {
      id: 1,
      name: 1,
      priority: 1,
      tenantId: 1,
      createdOn: 1,
      count: 1,
      assigned: 1
    }).lean();
    res.json(products);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/assignments', async (req, res) => {
  try {
    const assignments = await Assignment.find({}, {
      agentId: 1, 
      productId: 1, 
      assignedOn: 1, 
      completed: 1,
      completedOn: 1,
      unassignedTime: 1,
      unassignedBy: 1
    }).lean();
    res.json(assignments);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// New combined endpoint to fetch all necessary data in one request
app.get('/api/dashboard-data', async (req, res) => {
  try {
    // Check cache first
    const cachedData = cache.get('dashboard-data');
    if (cachedData) {
      return res.json(cachedData);
    }

    const [agents, products, assignments] = await Promise.all([
      Agent.find({}, {
        name: 1,
        role: 1,
        capacity: 1
      }).lean(),
      Product.find({}, {
        id: 1,
        name: 1,
        priority: 1,
        tenantId: 1,
        createdOn: 1,
        count: 1,
        assigned: 1
      }).lean(),
      Assignment.find({}, {
        agentId: 1, 
        productId: 1, 
        assignedOn: 1, 
        completed: 1,
        completedOn: 1,
        unassignedTime: 1,
        unassignedBy: 1
      }).lean()
    ]);
    
    const responseData = {
      agents,
      products,
      assignments,
      totalAgents: agents.length,
      totalProducts: products.length,
      totalAssignments: assignments.length
    };

    // Cache the data (short TTL since it changes frequently)
    cache.set('dashboard-data', responseData, 30000); // 30 seconds TTL
    
    res.json(responseData);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Completed assignments
app.get('/api/completed-assignments', async (req, res) => {
  try {
    const completed = await Assignment.find(
      { completed: true },
      { agentId: 1, productId: 1, assignedOn: 1, completedOn: 1 }
    ).lean();
    res.json(completed);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Unassigned products
app.get('/api/unassigned-products', async (req, res) => {
  try {
    const unassigned = await Product.find(
      { assigned: false },
      { id: 1, name: 1, count: 1, tenantId: 1, priority: 1, createdOn: 1 }
    ).lean();
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
    }).lean();
    
    // Get unique product IDs to minimize database lookups
    const productIds = [...new Set(prev.map(a => a.productId))];
    
    // Fetch all required products in a single query
    const productsMap = {};
    const products = await Product.find(
      { id: { $in: productIds } },
      { id: 1, count: 1, tenantId: 1, priority: 1, createdOn: 1 }
    ).lean();
    
    // Create a lookup map for faster access
    products.forEach(p => {
      productsMap[p.id] = p;
    });
    
    // Prepare the result
    const result = prev.map(a => {
      const product = productsMap[a.productId];
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
    
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Queue: all products
app.get('/api/queue', async (req, res) => {
  try {
    const products = await Product.find({}, {
      id: 1,
      name: 1,
      priority: 1,
      tenantId: 1,
      createdOn: 1,
      count: 1,
      assigned: 1
    }).lean();
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
          rows.push(row);
        })
        .on('end', resolve)
        .on('error', reject);
    });

    // Process in batches for large files
    const BATCH_SIZE = 500;
    let processed = 0;

    while (processed < rows.length) {
      const batch = rows.slice(processed, processed + BATCH_SIZE);
      processed += batch.length;

      const bulkOps = [];
      for (const row of batch) {
        const productId = row['abstract_product_id'] || row['item.abstract_product_id'];
        if (!productId) continue;
        bulkOps.push({
          updateOne: {
            filter: { id: productId },
            update: {
              $set: {
                id: productId,
                name: productId,
                priority: row['rule_priority'] || null,
                tenantId: row['tenant_id'] || null,
                createdOn: row['oldest_created_on'] || null,
                count: Number(row['count']) || 1,
                assigned: false
              }
            },
            upsert: true
          }
        });
      }
      if (bulkOps.length > 0) {
        await Product.bulkWrite(bulkOps);
        console.log(`BulkWrite processed ${bulkOps.length} product updates (batch ${processed/BATCH_SIZE})`);
      }
    }

    // Clear caches to ensure fresh data
    cache.invalidate();
    
    await fs.unlink(req.file.path);
    res.status(200).json({ message: 'CSV uploaded and products updated successfully' });
  } catch (error) {
    console.error('Error uploading CSV:', error);
    res.status(500).json({ error: error.message });
  }
});

// Refresh endpoint
app.post('/api/refresh', async (req, res) => {
  try {
    await loadData();
    // Clear caches to ensure fresh data
    cache.invalidate();
    res.status(200).json({ message: 'Data refreshed successfully' });
  } catch (error) {
    res.status(500).json({ error: 'Failed to refresh data' });
  }
});

// ------------------------------
// Task Assignment and Completion Endpoints
// ------------------------------
let assignmentInProgress = false;

// Assign a product to an agent (Optimized: directly query for available product)
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
    // Directly query for an available product (using assigned flag)
    const availableProduct = await Product.findOne({ assigned: false }).sort({ createdOn: 1 });
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
    
    // Invalidate cache
    cache.invalidate('dashboard-data');
    
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
    
    // Invalidate cache
    cache.invalidate('dashboard-data');
    
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
    
    // Get all product IDs to update
    const productIds = activeAssignments.map(a => a.productId);
    
    // Bulk update assignments
    await Assignment.updateMany(
      {
        agentId: agent._id,
        completed: false,
        unassignedTime: { $exists: false }
      },
      {
        $set: {
          completed: true,
          completedOn: new Date().toISOString().replace('T', ' ').substring(0, 19)
        }
      }
    );
    
    // Bulk update products
    await Product.updateMany(
      { id: { $in: productIds } },
      { $set: { assigned: false } }
    );
    
    // Invalidate cache
    cache.invalidate('dashboard-data');
    
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
    
    // Collect agent IDs for lookup
    const agentIds = productAssignments.map(a => a.agentId);
    const agents = await Agent.find({ _id: { $in: agentIds } }, { name: 1 }).lean();
    
    // Create agent lookup map
    const agentMap = {};
    agents.forEach(a => {
      agentMap[a._id] = a.name;
    });
    
    // Update assignments
    const now = new Date().toISOString().replace('T', ' ').substring(0, 19);
    await Assignment.updateMany(
      {
        productId,
        completed: false,
        unassignedTime: { $exists: false }
      },
      {
        $set: {
          unassignedTime: now,
          unassignedBy: agentMap[productAssignments[0].agentId] || 'Unknown'
        }
      }
    );
    
    const product = await Product.findOne({ id: productId });
    if (product) {
      product.assigned = false;
      await product.save();
    }
    
    // Invalidate cache
    cache.invalidate('dashboard-data');
    
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
    
    // Get product IDs to update
    const productIds = activeAssignments.map(a => a.productId);
    
    // Bulk update assignments
    await Assignment.updateMany(
      {
        agentId: agent._id,
        completed: false,
        unassignedTime: { $exists: false }
      },
      {
        $set: {
          unassignedTime: new Date().toISOString().replace('T', ' ').substring(0, 19),
          unassignedBy: agent.name
        }
      }
    );
    
    // Bulk update products
    await Product.updateMany(
      { id: { $in: productIds } },
      { $set: { assigned: false } }
    );
    
    // Invalidate cache
    cache.invalidate('dashboard-data');
    
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
    // Get all active assignments
    const activeAssignments = await Assignment.find({
      completed: false,
      unassignedTime: { $exists: false }
    });
    
    if (activeAssignments.length === 0) {
      return res.status(200).json({ message: 'No active assignments to unassign' });
    }
    
    // Collect agent IDs and product IDs
    const agentIds = [...new Set(activeAssignments.map(a => a.agentId))];
    const productIds = activeAssignments.map(a => a.productId);
    
    // Get agent names
    const agents = await Agent.find({ _id: { $in: agentIds } }, { name: 1 }).lean();
    const agentMap = {};
    agents.forEach(a => {
      agentMap[a._id] = a.name;
    });
    
    // Prepare unassignment data
    const now = new Date().toISOString().replace('T', ' ').substring(0, 19);
    
    // Bulk update all assignments
    const bulkOps = activeAssignments.map(a => ({
      updateOne: {
        filter: { _id: a._id },
        update: {
          $set: {
            unassignedTime: now,
            unassignedBy: agentMap[a.agentId] || 'Unknown'
          }
        }
      }
    }));
    
    await Assignment.bulkWrite(bulkOps);
    
    // Bulk update all products
    await Product.updateMany(
      { id: { $in: productIds } },
      { $set: { assigned: false } }
    );
    
    // Invalidate cache
    cache.invalidate('dashboard-data');
    
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
    const completed = await Assignment.find({ completed: true }).lean();
    
    // Get unique agent IDs
    const agentIds = [...new Set(completed.map(a => a.agentId))];
    
    // Fetch all agents in one query
    const agents = await Agent.find({ _id: { $in: agentIds } }, { name: 1 }).lean();
    
    // Create a map for easy lookup
    const agentMap = {};
    agents.forEach(a => {
      agentMap[a._id] = a.name;
    });
    
    res.setHeader('Content-disposition', 'attachment; filename=completed-tasks.csv');
    res.setHeader('Content-Type', 'text/csv');
    const csvStream = format({ headers: true });
    csvStream.pipe(res);
    for (const a of completed) {
      csvStream.write({
        assignmentId: a._id,
        agentId: a.agentId,
        completedBy: agentMap[a.agentId] || 'Unknown',
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
    const unassigned = await Product.find({ assigned: false }).lean();
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
    }).lean();
    
    // Get unique product IDs
    const productIds = [...new Set(prev.map(a => a.productId))];
    
    // Fetch all products in one query
    const productsMap = {};
    const products = await Product.find(
      { id: { $in: productIds } },
      { id: 1, count: 1, tenantId: 1, priority: 1, createdOn: 1 }
    ).lean();
    
    // Create lookup map
    products.forEach(p => {
      productsMap[p.id] = p;
    });
    
    // Prepare results
    const results = prev.map(a => {
      const product = productsMap[a.productId];
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
    const allProducts = await Product.find().lean();
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