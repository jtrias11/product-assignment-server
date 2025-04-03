/***************************************************************
 * server.js - MongoDB Integration with Enhanced Error Handling
 * 
 * Features:
 * - Loads agents from "Walmart BH Roster.xlsx" (column E).
 * - Loads products from "output.csv" (using abstract_product_id as primary).
 * - Uses MongoDB for persistent data storage.
 * - Includes error handling to catch and display issues during startup.
 ***************************************************************/

// Add error handlers at the top
process.on('uncaughtException', (err) => {
  console.error('UNCAUGHT EXCEPTION:', err);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('UNHANDLED PROMISE REJECTION:', reason);
});

console.log("Starting server.js...");
require('dotenv').config();
console.log("Loaded environment variables");

const express = require('express');
console.log("Loaded express");
const cors = require('cors');
console.log("Loaded cors");
const { v4: uuidv4 } = require('uuid');
console.log("Loaded uuid");
const fs = require('fs').promises;
console.log("Loaded fs.promises");
const path = require('path');
console.log("Loaded path");
const csvParser = require('csv-parser');
console.log("Loaded csv-parser");
const xlsx = require('xlsx');
console.log("Loaded xlsx");
const { createReadStream, createWriteStream } = require('fs');
console.log("Loaded createReadStream, createWriteStream");
const multer = require('multer');
console.log("Loaded multer");
const { format } = require('@fast-csv/format');
console.log("Loaded @fast-csv/format");
const mongoose = require('mongoose');
console.log("Loaded mongoose");

// Load models
const Agent = require('./models/Agent');
console.log("Loaded Agent model");
const Product = require('./models/Product');
console.log("Loaded Product model");
const Assignment = require('./models/Assignment');
console.log("Loaded Assignment model");

const app = express();
const PORT = process.env.PORT || 3001;
console.log(`Server will run on port ${PORT}`);

app.use(cors());
app.use(express.json());
console.log("Set up Express middleware");

// ------------------------------
// Data Storage
// ------------------------------
let agents = [];
let products = [];
let assignments = [];
console.log("Initialized data arrays");

// ------------------------------
// File Paths and Directories
// ------------------------------
const DATA_DIR = path.join(__dirname, 'data');
const AGENTS_FILE = path.join(DATA_DIR, 'agents.json');
const ASSIGNMENTS_FILE = path.join(DATA_DIR, 'assignments.json');
const OUTPUT_CSV = path.join(DATA_DIR, 'output.csv');
const ROSTER_EXCEL = path.join(DATA_DIR, 'Walmart BH Roster.xlsx');
console.log("Set up file paths");

// ------------------------------
// Multer Configuration
// ------------------------------
const storage = multer.diskStorage({
  destination: (req, file, cb) => { cb(null, DATA_DIR); },
  filename: (req, file, cb) => { cb(null, Date.now() + '-' + file.originalname); }
});
const upload = multer({ storage });
console.log("Configured multer for file uploads");

// ------------------------------
// MongoDB Connection
// ------------------------------
const connectDB = async () => {
  console.log("Attempting to connect to MongoDB...");
  try {
    await mongoose.connect(process.env.MONGO_URI);
    console.log('MongoDB Connected!');
    return true;
  } catch (error) {
    console.error('MongoDB Connection Error:', error);
    return false;
  }
};

// ------------------------------
// Helper Functions
// ------------------------------
async function ensureDataDir() {
  console.log("Ensuring data directory exists...");
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    console.log('Data directory is ready');
    return true;
  } catch (error) {
    console.error('Error creating data directory:', error);
    return false;
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
  console.log("Starting to load data...");
  try {
    console.log("Ensuring data directory exists...");
    await ensureDataDir();
    
    // Load agents
    console.log("Checking for agents in MongoDB...");
    let agentsFromDB = await Agent.find();
    if (agentsFromDB.length === 0) {
      console.log("No agents found in MongoDB, looking for agents in files...");
      try {
        const agentsData = await fs.readFile(AGENTS_FILE, 'utf8');
        agents = JSON.parse(agentsData);
        console.log(`Loaded ${agents.length} agents from JSON file`);
        
        // Save agents to MongoDB
        console.log("Saving agents to MongoDB...");
        for (const agent of agents) {
          await Agent.create(agent);
        }
        console.log(`Saved ${agents.length} agents to MongoDB`);
      } catch (error) {
        console.log('No agents JSON file found, importing from Excel roster...');
        const excelAgents = await readRosterExcel();
        if (excelAgents.length > 0) {
          agents = excelAgents;
          // Save agents to MongoDB
          console.log("Saving Excel agents to MongoDB...");
          for (const agent of agents) {
            await Agent.create(agent);
          }
          await saveAgents();
        } else {
          console.log("Creating sample agents...");
          agents = [
            { id: 1, name: "Agent Sample 1", role: "Item Review", capacity: 10, currentAssignments: [] },
            { id: 2, name: "Agent Sample 2", role: "Item Review", capacity: 10, currentAssignments: [] }
          ];
          // Save sample agents to MongoDB
          for (const agent of agents) {
            await Agent.create(agent);
          }
          await saveAgents();
        }
      }
    } else {
      console.log(`Found ${agentsFromDB.length} agents in MongoDB`);
      agents = agentsFromDB;
      console.log(`Loaded ${agents.length} agents from MongoDB`);
    }
    
    // Load products from CSV and save to MongoDB if needed
    console.log("Checking for products in MongoDB...");
    let productsFromDB = await Product.find();
    if (productsFromDB.length === 0) {
      console.log("No products found in MongoDB, loading from CSV...");
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
        
        // Save products to MongoDB in batches to avoid timeout
        console.log("Saving products to MongoDB in batches...");
        const batchSize = 100;
        for (let i = 0; i < products.length; i += batchSize) {
          const batch = products.slice(i, i + batchSize);
          await Product.insertMany(batch);
          console.log(`Saved batch ${Math.floor(i/batchSize) + 1} of products to MongoDB`);
        }
        console.log("All product batches saved to MongoDB");
      } catch (error) {
        console.log('Error loading products from output CSV:', error);
        products = [];
      }
    } else {
      console.log(`Found ${productsFromDB.length} products in MongoDB`);
      products = productsFromDB;
      console.log(`Loaded ${products.length} products from MongoDB`);
    }
    
    // Load assignments
    console.log("Checking for assignments in MongoDB...");
    let assignmentsFromDB = await Assignment.find();
    if (assignmentsFromDB.length === 0) {
      console.log("No assignments found in MongoDB, looking for assignments in files...");
      try {
        const assignmentsData = await fs.readFile(ASSIGNMENTS_FILE, 'utf8');
        assignments = JSON.parse(assignmentsData);
        console.log(`Loaded ${assignments.length} assignments from file`);
        
        // Save assignments to MongoDB
        if (assignments.length > 0) {
          console.log("Saving assignments to MongoDB...");
          await Assignment.insertMany(assignments);
          console.log(`Saved ${assignments.length} assignments to MongoDB`);
        }
        await updateAgentAssignments();
      } catch (error) {
        console.log('No assignments file found, initializing with empty array');
        assignments = [];
        await saveAssignments();
      }
    } else {
      console.log(`Found ${assignmentsFromDB.length} assignments in MongoDB`);
      assignments = assignmentsFromDB;
      console.log(`Loaded ${assignments.length} assignments from MongoDB`);
      await updateAgentAssignments();
    }
    
    console.log("Data loading completed successfully");
    return true;
  } catch (error) {
    console.error('Error loading data:', error);
    return false;
  }
}

async function updateAgentAssignments() {
  console.log("Updating agent assignments...");
  try {
    // Reset all agent assignments in memory
    agents.forEach(agent => { agent.currentAssignments = []; });
    
    // Update agent assignments in memory
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
    
    // Update agents in MongoDB
    console.log("Saving updated agent assignments to MongoDB...");
    for (const agent of agents) {
      await Agent.findOneAndUpdate({ id: agent.id }, { currentAssignments: agent.currentAssignments });
    }
    console.log("Agent assignments updated in MongoDB");
    return true;
  } catch (error) {
    console.error("Error updating agent assignments:", error);
    return false;
  }
}

// ------------------------------
// Save Functions
// ------------------------------
async function saveAgents() {
  console.log("Saving agents to MongoDB...");
  try {
    // For each agent in memory, update or create in MongoDB
    for (const agent of agents) {
      await Agent.findOneAndUpdate(
        { id: agent.id }, 
        agent, 
        { upsert: true, new: true }
      );
    }
    console.log('Agents saved to MongoDB');
    return true;
  } catch (error) {
    console.error('Error saving agents to MongoDB:', error);
    return false;
  }
}

async function saveAssignments() {
  console.log("Saving assignments to MongoDB...");
  try {
    // Clear existing assignments
    await Assignment.deleteMany({});
    // Insert all assignments
    if (assignments.length > 0) {
      await Assignment.insertMany(assignments);
    }
    console.log('Assignments saved to MongoDB');
    return true;
  } catch (error) {
    console.error('Error saving assignments to MongoDB:', error);
    return false;
  }
}

// ------------------------------
// API Routes
// ------------------------------
app.get('/', (req, res) => { 
  console.log("Root endpoint accessed");
  res.send('Product Assignment Server is running'); 
});

// Get all agents
app.get('/api/agents', async (req, res) => { 
  console.log("Getting all agents");
  try {
    const dbAgents = await Agent.find();
    console.log(`Returning ${dbAgents.length} agents`);
    res.json(dbAgents); 
  } catch (error) {
    console.error('Error fetching agents:', error);
    res.status(500).json({ error: 'Failed to fetch agents' });
  }
});

// Get all products
app.get('/api/products', async (req, res) => { 
  console.log("Getting all products");
  try {
    const dbProducts = await Product.find();
    console.log(`Returning ${dbProducts.length} products`);
    res.json(dbProducts); 
  } catch (error) {
    console.error('Error fetching products:', error);
    res.status(500).json({ error: 'Failed to fetch products' });
  }
});

// Get all assignments
app.get('/api/assignments', async (req, res) => { 
  console.log("Getting all assignments");
  try {
    const dbAssignments = await Assignment.find();
    console.log(`Returning ${dbAssignments.length} assignments`);
    res.json(dbAssignments); 
  } catch (error) {
    console.error('Error fetching assignments:', error);
    res.status(500).json({ error: 'Failed to fetch assignments' });
  }
});

// Completed assignments endpoint
app.get('/api/completed-assignments', async (req, res) => {
  console.log("Getting completed assignments");
  try {
    const completed = await Assignment.find({ completed: true });
    console.log(`Returning ${completed.length} completed assignments`);
    res.json(completed);
  } catch (error) {
    console.error('Error fetching completed assignments:', error);
    res.status(500).json({ error: 'Failed to fetch completed assignments' });
  }
});

// Unassigned products endpoint
app.get('/api/unassigned-products', async (req, res) => {
  console.log("Getting unassigned products");
  try {
    const unassigned = await Product.find({ assigned: false });
    console.log(`Returning ${unassigned.length} unassigned products`);
    res.json(unassigned);
  } catch (error) {
    console.error('Error fetching unassigned products:', error);
    res.status(500).json({ error: 'Failed to fetch unassigned products' });
  }
});

// Previously assigned (unassigned or completed) endpoint
app.get('/api/previously-assigned', async (req, res) => {
  console.log("Getting previously assigned products");
  try {
    // Find all assignments that are either completed or unassigned
    const prevAssignments = await Assignment.find({
      $or: [{ completed: true }, { unassignedTime: { $exists: true } }]
    });
    console.log(`Found ${prevAssignments.length} previously assigned products`);
    
    // Get product details for each assignment
    const prev = await Promise.all(prevAssignments.map(async (a) => {
      const product = await Product.findOne({ id: a.productId });
      return {
        id: product ? product.id : a.productId,
        count: product ? product.count : '',
        tenantId: product ? product.tenantId : '',
        priority: product ? product.priority : '',
        createdOn: product ? product.createdOn : '',
        unassignedTime: a.unassignedTime || '',
        unassignedBy: a.unassignedBy || ''
      };
    }));
    
    console.log(`Returning ${prev.length} previously assigned products`);
    res.json(prev);
  } catch (error) {
    console.error('Error fetching previously assigned products:', error);
    res.status(500).json({ error: 'Failed to fetch previously assigned products' });
  }
});

// Queue endpoint: return all products
app.get('/api/queue', async (req, res) => {
  console.log("Getting product queue");
  try {
    const allProducts = await Product.find();
    console.log(`Returning ${allProducts.length} products in queue`);
    res.json(allProducts);
  } catch (error) {
    console.error('Error fetching product queue:', error);
    res.status(500).json({ error: 'Failed to fetch product queue' });
  }
});

// File Upload Endpoint (Merge CSV)
app.post('/api/upload-output', upload.single('outputFile'), async (req, res) => {
  console.log('File upload received:', req.file);
  try {
    const newData = await new Promise((resolve, reject) => {
      const results = [];
      createReadStream(req.file.path)
        .pipe(csvParser())
        .on('data', (row) => results.push(row))
        .on('end', () => resolve(results))
        .on('error', reject);
    });
    console.log(`Parsed ${newData.length} rows from uploaded file`);
    
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
      console.log(`Found ${existingData.length} rows in existing CSV`);
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
    console.log(`Created merged data with ${mergedData.length} rows`);
    
    // Update the CSV file
    console.log("Writing merged data to CSV file...");
    const ws = createWriteStream(OUTPUT_CSV);
    const csvStream = format({ headers: true });
    csvStream.pipe(ws);
    mergedData.forEach(row => csvStream.write(row));
    csvStream.end();
    
    // Update MongoDB with new product data
    console.log("Updating MongoDB with merged product data...");
    const productUpdates = mergedData.map(row => {
      const productId = row.abstract_product_id || row.item_abstract_product_id || row['item.abstract_product_id'] || row.product_id;
      return {
        id: productId,
        name: row.product_name || "",
        priority: row.rule_priority || row.priority,
        tenantId: row.tenant_id,
        createdOn: row.oldest_created_on || row.sys_created_on || row.created_on,
        count: row.count,
        assigned: false
      };
    }).filter(p => p.id);
    
    // Use upsert to update existing or insert new products
    console.log(`Upserting ${productUpdates.length} products to MongoDB...`);
    for (const product of productUpdates) {
      await Product.findOneAndUpdate(
        { id: product.id },
        product,
        { upsert: true, new: true }
      );
    }
    
    await fs.unlink(req.file.path);
    console.log("Temporary upload file deleted");
    
    await loadData();
    console.log("Data refreshed after upload");
    
    res.status(200).json({ message: 'Output CSV uploaded and merged successfully. Data refreshed.' });
  } catch (error) {
    console.error('Error uploading output CSV:', error);
    res.status(500).json({ error: error.message });
  }
});

// Refresh endpoint
app.post('/api/refresh', async (req, res) => {
  console.log("Refresh data endpoint called");
  try {
    await loadData();
    console.log('Data refreshed successfully from MongoDB');
    res.status(200).json({ message: 'Data refreshed successfully' });
  } catch (error) {
    console.error('Error refreshing data:', error);
    res.status(500).json({ error: 'Failed to refresh data' });
  }
});

// Assign endpoint
let assignmentInProgress = false;
app.post('/api/assign', async (req, res) => {
  console.log("Assign task endpoint called");
  if (assignmentInProgress) {
    console.log("Another assignment is already in progress");
    return res.status(409).json({ error: 'Another assignment is in progress, please try again later' });
  }
  assignmentInProgress = true;
  try {
    const { agentId } = req.body;
    console.log(`Assigning task to agent ID: ${agentId}`);
    
    if (!agentId) {
      assignmentInProgress = false;
      return res.status(400).json({ error: 'Agent ID is required' });
    }
    
    const agent = await Agent.findOne({ id: agentId });
    if (!agent) {
      assignmentInProgress = false;
      return res.status(404).json({ error: 'Agent not found' });
    }
    
    if (agent.currentAssignments.length >= agent.capacity) {
      assignmentInProgress = false;
      return res.status(400).json({ error: 'Agent has reached maximum capacity' });
    }
    
    // Get assigned product IDs
    const existingAssignments = await Assignment.find({});
    const assignedProductIds = new Set();
    existingAssignments.forEach(a => assignedProductIds.add(a.productId));
    console.log(`Found ${assignedProductIds.size} already assigned products`);
    
    // Find available products
    const allProducts = await Product.find({});
    const availableProducts = allProducts
      .filter(p => !p.assigned && !assignedProductIds.has(p.id))
      .sort((a, b) => new Date(a.createdOn) - new Date(b.createdOn));
    console.log(`Found ${availableProducts.length} available products`);
      
    if (availableProducts.length === 0) {
      assignmentInProgress = false;
      return res.status(404).json({ error: 'No available products to assign' });
    }
    
    const productToAssign = availableProducts[0];
    console.log(`Selected product for assignment: ${productToAssign.id}`);
    
    // Update product status in MongoDB
    await Product.findOneAndUpdate(
      { id: productToAssign.id },
      { assigned: true }
    );
    console.log(`Updated product ${productToAssign.id} status to assigned`);
    
    // Create new assignment
    const newAssignment = new Assignment({
      id: uuidv4(),
      agentId: agent.id,
      productId: productToAssign.id,
      assignedOn: new Date().toISOString().replace('T', ' ').substring(0, 19),
      completed: false,
      completedOn: null
    });
    
    await newAssignment.save();
    console.log(`Created new assignment with ID: ${newAssignment.id}`);
    
    // Update in-memory data
    productToAssign.assigned = true;
    assignments.push(newAssignment.toObject());
    await updateAgentAssignments();
    
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
  console.log("Complete task endpoint called");
  try {
    const { agentId, productId } = req.body;
    console.log(`Completing task for agent ${agentId}, product ${productId}`);
    
    if (!agentId || !productId) {
      return res.status(400).json({ error: 'agentId and productId are required' });
    }
    
    const agent = await Agent.findOne({ id: agentId });
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }
    
    const assignment = await Assignment.findOne({
      agentId: agentId,
      productId: productId,
      completed: false,
      unassignedTime: { $exists: false }
    });
    
    if (!assignment) {
      return res.status(404).json({ error: 'Active assignment not found' });
    }
    
    // Update assignment in MongoDB
    assignment.completed = true;
    assignment.completedOn = new Date().toISOString().replace('T', ' ').substring(0, 19);
    await assignment.save();
    console.log(`Marked assignment ${assignment.id} as completed`);
    
    // Update product in MongoDB
    await Product.findOneAndUpdate(
      { id: productId },
      { assigned: false }
    );
    console.log(`Updated product ${productId} status to unassigned`);
    
    // Update in-memory arrays
    const assignmentIndex = assignments.findIndex(a =>
      a.agentId === agentId && a.productId === productId && !a.completed && !a.unassignedTime
    );
    
    if (assignmentIndex !== -1) {
      assignments[assignmentIndex].completed = true;
      assignments[assignmentIndex].completedOn = assignment.completedOn;
    }
    
    const product = products.find(p => p.id === productId);
    if (product) {
      product.assigned = false;
    }
    
    await updateAgentAssignments();
    
    res.status(200).json({ message: `Task ${productId} completed by ${agent.name}` });
  } catch (error) {
    console.error('Error completing task:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Complete All Tasks endpoint
app.post('/api/complete-all-agent', async (req, res) => {
  console.log("Complete all tasks endpoint called");
  try {
    const { agentId } = req.body;
    console.log(`Completing all tasks for agent ${agentId}`);
    
    if (!agentId) {
      return res.status(400).json({ error: 'Agent ID is required' });
    }
    
    const agent = await Agent.findOne({ id: agentId });
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }
    
    const activeAssignments = await Assignment.find({
      agentId: agentId,
      completed: false,
      unassignedTime: { $exists: false }
    });
    console.log(`Found ${activeAssignments.length} active assignments for agent`);
    
    if (activeAssignments.length === 0) {
      return res.status(200).json({ message: 'No active tasks to complete for this agent' });
    }
    
    // Update all assignments and products in MongoDB
    const completedTime = new Date().toISOString().replace('T', ' ').substring(0, 19);
    
    for (const assignment of activeAssignments) {
      assignment.completed = true;
      assignment.completedOn = complete
assignment.completed = true;
      assignment.completedOn = completedTime;
      await assignment.save();
      console.log(`Marked assignment ${assignment.id} as completed`);
      
      await Product.findOneAndUpdate(
        { id: assignment.productId },
        { assigned: false }
      );
      console.log(`Updated product ${assignment.productId} status to unassigned`);
      
      // Update in-memory assignments
      const inMemoryAssignment = assignments.find(a => 
        a.agentId === assignment.agentId && a.productId === assignment.productId
      );
      
      if (inMemoryAssignment) {
        inMemoryAssignment.completed = true;
        inMemoryAssignment.completedOn = completedTime;
      }
      
      // Update in-memory products
      const product = products.find(p => p.id === assignment.productId);
      if (product) {
        product.assigned = false;
      }
    }
    
    await updateAgentAssignments();
    
    res.status(200).json({ message: `Completed all (${activeAssignments.length}) tasks for agent ${agent.name}` });
  } catch (error) {
    console.error('Error completing all tasks for agent:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Unassign a single product.
app.post('/api/unassign-product', async (req, res) => {
  console.log("Unassign product endpoint called");
  try {
    const { productId, agentId } = req.body;
    console.log(`Unassigning product ${productId}`);
    
    if (!productId) {
      return res.status(400).json({ error: 'Product ID is required' });
    }
    
    const productAssignments = await Assignment.find({
      productId: productId,
      completed: false,
      unassignedTime: { $exists: false }
    });
    console.log(`Found ${productAssignments.length} active assignments for this product`);
    
    if (productAssignments.length === 0) {
      return res.status(404).json({ error: 'No active assignment found for this product' });
    }
    
    const unassignedTime = new Date().toISOString().replace('T', ' ').substring(0, 19);
    
    for (const assignment of productAssignments) {
      const agent = await Agent.findOne({ id: assignment.agentId });
      
      // Update assignment in MongoDB
      assignment.unassignedTime = unassignedTime;
      assignment.unassignedBy = agent ? agent.name : 'Unknown';
      assignment.wasUnassigned = true;
      await assignment.save();
      console.log(`Marked assignment ${assignment.id} as unassigned`);
      
      // Update in-memory assignment
      const inMemoryAssignment = assignments.find(a => 
        a.agentId === assignment.agentId && a.productId === assignment.productId
      );
      
      if (inMemoryAssignment) {
        inMemoryAssignment.unassignedTime = unassignedTime;
        inMemoryAssignment.unassignedBy = agent ? agent.name : 'Unknown';
        inMemoryAssignment.wasUnassigned = true;
      }
    }
    
    // Update product in MongoDB
    await Product.findOneAndUpdate(
      { id: productId },
      { assigned: false }
    );
    console.log(`Updated product ${productId} status to unassigned`);
    
    // Update in-memory product
    const product = products.find(p => p.id === productId);
    if (product) {
      product.assigned = false;
    }
    
    await updateAgentAssignments();
    
    res.status(200).json({ message: `Product ${productId} unassigned successfully` });
  } catch (error) {
    console.error('Error unassigning product:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Unassign all tasks from a specific agent.
app.post('/api/unassign-agent', async (req, res) => {
  console.log("Unassign agent tasks endpoint called");
  try {
    const { agentId } = req.body;
    console.log(`Unassigning all tasks from agent ${agentId}`);
    
    if (!agentId) {
      return res.status(400).json({ error: 'Agent ID is required' });
    }
    
    const agent = await Agent.findOne({ id: agentId });
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }
    
    const tasksCount = agent.currentAssignments.length;
    if (tasksCount === 0) {
      return res.status(200).json({ message: 'Agent has no tasks to unassign' });
    }
    
    // Update products
    console.log(`Updating ${tasksCount} products to unassigned`);
    for (const task of agent.currentAssignments) {
      await Product.findOneAndUpdate(
        { id: task.productId },
        { assigned: false }
      );
      
      // Also update in-memory products
      const product = products.find(p => p.id === task.productId);
      if (product) {
        product.assigned = false;
      }
    }
    
    // Update assignments
    const unassignedTime = new Date().toISOString().replace('T', ' ').substring(0, 19);
    
    const agentAssignments = await Assignment.find({
      agentId: agentId,
      completed: false,
      unassignedTime: { $exists: false }
    });
    console.log(`Found ${agentAssignments.length} active assignments for this agent`);
    
    for (const assignment of agentAssignments) {
      assignment.unassignedTime = unassignedTime;
      assignment.unassignedBy = agent.name;
      assignment.wasUnassigned = true;
      await assignment.save();
      
      // Update in-memory assignments
      const inMemoryAssignment = assignments.find(a => 
        a.agentId === assignment.agentId && a.productId === assignment.productId
      );
      
      if (inMemoryAssignment) {
        inMemoryAssignment.unassignedTime = unassignedTime;
        inMemoryAssignment.unassignedBy = agent.name;
        inMemoryAssignment.wasUnassigned = true;
      }
    }
    
    // Clear agent's current assignments in MongoDB
    agent.currentAssignments = [];
    await agent.save();
    console.log(`Cleared agent ${agentId} assignments`);
    
    await updateAgentAssignments();
    
    res.status(200).json({ message: `Unassigned ${tasksCount} tasks from agent ${agent.name}` });
  } catch (error) {
    console.error('Error unassigning agent tasks:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Unassign all tasks from all agents.
app.post('/api/unassign-all', async (req, res) => {
  console.log("Unassign all tasks endpoint called");
  try {
    // Find all active assignments
    const activeAssignments = await Assignment.find({ 
      completed: false, 
      unassignedTime: { $exists: false } 
    });
    console.log(`Found ${activeAssignments.length} active assignments to unassign`);
    
    const totalActive = activeAssignments.length;
    const unassignedTime = new Date().toISOString().replace('T', ' ').substring(0, 19);
    
    // Update all assignments in MongoDB
    for (const assignment of activeAssignments) {
      const agent = await Agent.findOne({ id: assignment.agentId });
      
      assignment.unassignedTime = unassignedTime;
      assignment.unassignedBy = agent ? agent.name : 'Unknown';
      assignment.wasUnassigned = true;
      await assignment.save();
      
      // Update in-memory assignments
      const inMemoryAssignment = assignments.find(a => 
        a.agentId === assignment.agentId && a.productId === assignment.productId
      );
      
      if (inMemoryAssignment) {
        inMemoryAssignment.unassignedTime = unassignedTime;
        inMemoryAssignment.unassignedBy = agent ? agent.name : 'Unknown';
        inMemoryAssignment.wasUnassigned = true;
      }
    }
    
    // Update all products in MongoDB
    console.log("Updating all assigned products to unassigned status");
    await Product.updateMany(
      { assigned: true },
      { assigned: false }
    );
    
    // Update in-memory products
    products.forEach(p => { p.assigned = false; });
    
    // Clear agent assignments
    console.log("Clearing all agent assignments");
    await Agent.updateMany(
      {},
      { currentAssignments: [] }
    );
    
    // Update in-memory agents
    agents.forEach(a => { a.currentAssignments = []; });
    
    res.status(200).json({ message: `Unassigned ${totalActive} tasks from all agents` });
  } catch (error) {
    console.error('Error unassigning all tasks:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Download endpoints
app.get('/api/download/completed-assignments', async (req, res) => {
  console.log("Download completed assignments endpoint called");
  try {
    const completed = await Assignment.find({ completed: true });
    console.log(`Found ${completed.length} completed assignments to download`);
    
    res.setHeader('Content-disposition', 'attachment; filename=completed-tasks.csv');
    res.setHeader('Content-Type', 'text/csv');
    
    const csvStream = format({ headers: true });
    csvStream.pipe(res);
    
    for (const a of completed) {
      const agent = await Agent.findOne({ id: a.agentId });
      csvStream.write({
        assignmentId: a.id,
        agentId: a.agentId,
        completedBy: agent ? agent.name : 'Unknown',
        productId: a.productId,
        assignedOn: a.assignedOn,
        completedOn: a.completedOn
      });
    }
    
    csvStream.end();
    console.log("CSV stream ended");
  } catch (error) {
    console.error('Error generating CSV for completed assignments:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

app.get('/api/download/unassigned-products', async (req, res) => {
  console.log("Download unassigned products endpoint called");
  try {
    const unassigned = await Product.find({ assigned: false });
    console.log(`Found ${unassigned.length} unassigned products to download`);
    
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
    console.log("CSV stream ended");
  } catch (error) {
    console.error('Error generating CSV for unassigned products:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

app.get('/api/download/previously-assigned', async (req, res) => {
  console.log("Download previously assigned endpoint called");
  try {
    const prevAssignments = await Assignment.find({
      $or: [{ completed: true }, { unassignedTime: { $exists: true } }]
    });
    console.log(`Found ${prevAssignments.length} previously assigned items to download`);
    
    const prev = await Promise.all(prevAssignments.map(async (a) => {
      const product = await Product.findOne({ id: a.productId });
      return {
        productId: product ? product.id : a.productId,
        count: product ? product.count : '',
        tenantId: product ? product.tenantId : '',
        priority: product ? product.priority : '',
        createdOn: product ? product.createdOn : '',
        unassignedTime: a.unassignedTime || '',
        unassignedBy: a.unassignedBy || ''
      };
    }));
    
    res.setHeader('Content-disposition', 'attachment; filename=previously-assigned.csv');
    res.setHeader('Content-Type', 'text/csv');
    
    const csvStream = format({ headers: true });
    csvStream.pipe(res);
    prev.forEach(row => csvStream.write(row));
    csvStream.end();
    console.log("CSV stream ended");
  } catch (error) {
    console.error('Error generating CSV for previously assigned products:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

app.get('/api/download/queue', async (req, res) => {
  console.log("Download queue endpoint called");
  try {
    const allProducts = await Product.find();
    console.log(`Found ${allProducts.length} products in queue to download`);
    
    res.setHeader('Content-disposition', 'attachment; filename=product-queue.csv');
    res.setHeader('Content-Type', 'text/csv');
    
    const csvStream = format({ headers: true });
    csvStream.pipe(res);
    
    allProducts.forEach(p => {
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
    console.log("CSV stream ended");
  } catch (error) {
    console.error('Error generating CSV for product queue:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Start the server
console.log("Starting server initialization...");
const server = app.listen(PORT, async () => {
  try {
    console.log(`Server running on port ${PORT}`);
    console.log("Connecting to MongoDB...");
    await connectDB();
    console.log("Loading data...");
    await loadData();
    console.log("Server is ready to handle requests");
  } catch (error) {
    console.error("Server startup error:", error);
  }
});

server.on('error', (error) => {
  console.error('Server error:', error);
});