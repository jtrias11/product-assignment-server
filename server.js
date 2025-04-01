/***************************************************************
 * server.js - Complete Script
 * 
 * Features:
 * - Loads agents from "Walmart BH Roster.xlsx" (column E).
 * - Loads products from output.csv incrementally (adds instead of replaces)
 * - Tracks assignment history to show previously assigned items
 * - Provides expanded queue view with all product details
 * - Handles file uploads and data refresh
 ***************************************************************/
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs').promises;
const path = require('path');
const csvParser = require('csv-parser');
const xlsx = require('xlsx');
const { createReadStream } = require('fs');
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
let completedAssignments = []; // Store completed assignments separately

// ------------------------------
// File Paths and Directories
// ------------------------------
const DATA_DIR = path.join(__dirname, 'data');
const AGENTS_FILE = path.join(DATA_DIR, 'agents.json');
const PRODUCTS_FILE = path.join(DATA_DIR, 'products.json');
const ASSIGNMENTS_FILE = path.join(DATA_DIR, 'assignments.json');
const COMPLETED_FILE = path.join(DATA_DIR, 'completed.json');
const OUTPUT_CSV = path.join(DATA_DIR, 'output.csv');
const ROSTER_EXCEL = path.join(DATA_DIR, 'Walmart BH Roster.xlsx');

// ------------------------------
// Multer Configuration
// ------------------------------
const storage = multer.diskStorage({
  destination: (req, file, cb) => { cb(null, DATA_DIR); },
  filename: (req, file, cb) => { 
    // Use a timestamp to avoid overwriting the original file
    const timestamp = Date.now();
    cb(null, `upload-${timestamp}.csv`); 
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

// Reads products from CSV file
function readCsvFile(filePath) {
  return new Promise((resolve) => {
    let results = [];
    console.log(`Reading CSV from: ${filePath}`);
    
    createReadStream(filePath)
      .pipe(csvParser())
      .on('data', (row) => results.push(row))
      .on('end', () => {
        console.log(`Read ${results.length} rows from CSV file.`);
        if(results.length > 0) {
          console.log("First row sample:", results[0]);
        }
        resolve(results);
      })
      .on('error', (error) => {
        console.error('Error reading CSV file:', error);
        resolve([]);
      });
  });
}

// Function to create product object from CSV row
function createProductFromCsvRow(row) {
  return {
    id: row.abstract_product_id || row.item_abstract_product_id || row['item.abstract_product_id'],
    name: row.name || row.product_name || row.description || "",
    priority: row.rule_priority || row.priority || "P3",
    tenantId: row.tenant_id || row.TenantID || "",
    createdOn: row.oldest_created_on || row.sys_created_on || row.created_on || new Date().toISOString(),
    count: parseInt(row.count) || 1,
    assigned: false,
    wasAssigned: false, // Track if this product was ever assigned
    unassignedTime: null, // Track when it was last unassigned
    unassignedBy: null, // Track who unassigned it (agent ID or "all")
    originalRow: { ...row } // Store all original fields for full queue view
  };
}

// New function to merge products from CSV with existing products
async function mergeProductsFromCsv(filePath) {
  try {
    // Read new products from the uploaded CSV
    const csvRows = await readCsvFile(filePath);
    
    // Count how many new products we're adding
    let newProductCount = 0;
    let updatedProductCount = 0;
    
    // Process each row from the CSV
    csvRows.forEach(row => {
      // Get the product ID from the row
      const productId = row.abstract_product_id || row.item_abstract_product_id || row['item.abstract_product_id'];
      
      // Skip if no valid product ID
      if (!productId) return;
      
      // Check if this product already exists
      const existingProductIndex = products.findIndex(p => p.id === productId);
      
      if (existingProductIndex === -1) {
        // This is a new product - add it
        products.push(createProductFromCsvRow(row));
        newProductCount++;
      } else {
        // This product already exists - update its fields if needed
        // Only update if the product is not currently assigned
        if (!products[existingProductIndex].assigned) {
          // Update fields but preserve assignment history
          const wasAssigned = products[existingProductIndex].wasAssigned;
          const unassignedTime = products[existingProductIndex].unassignedTime;
          const unassignedBy = products[existingProductIndex].unassignedBy;
          
          // Create updated product object
          const updatedProduct = createProductFromCsvRow(row);
          
          // Preserve assignment history
          updatedProduct.wasAssigned = wasAssigned;
          updatedProduct.unassignedTime = unassignedTime;
          updatedProduct.unassignedBy = unassignedBy;
          
          // Replace the product in the array
          products[existingProductIndex] = updatedProduct;
          updatedProductCount++;
        }
      }
    });
    
    console.log(`Added ${newProductCount} new products and updated ${updatedProductCount} existing products`);
    return { newProducts: newProductCount, updatedProducts: updatedProductCount };
  } catch (error) {
    console.error('Error merging products from CSV:', error);
    throw error;
  }
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
async function loadData(initialLoad = true) {
  try {
    await ensureDataDir();
    
    // 1. Load agents from JSON (if available) or Excel.
    if (initialLoad) {
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
    }
    
    // 2. Load products from JSON file or initial CSV
    if (initialLoad) {
      try {
        // First try to load from JSON file if available
        const productsData = await fs.readFile(PRODUCTS_FILE, 'utf8');
        products = JSON.parse(productsData);
        console.log(`Loaded ${products.length} products from products.json`);
      } catch (error) {
        // If JSON file doesn't exist, load from CSV
        console.log('No products.json found, loading from output.csv...');
        try {
          // Check if output.csv exists
          await fs.access(OUTPUT_CSV);
          
          // Read initial products from CSV
          const csvRows = await readCsvFile(OUTPUT_CSV);
          products = csvRows.map(row => createProductFromCsvRow(row))
            .filter(p => p.id); // Filter out products with no ID
            
          console.log(`Loaded ${products.length} products from output CSV`);
          
          // Save products to JSON for faster loading next time
          await saveProducts();
        } catch (error) {
          console.log('No output.csv found or error loading products:', error);
          products = [];
        }
      }
    }
    
    // 3. Load assignments and completed assignments
    if (initialLoad) {
      try {
        const assignmentsData = await fs.readFile(ASSIGNMENTS_FILE, 'utf8');
        assignments = JSON.parse(assignmentsData);
        console.log(`Loaded ${assignments.length} assignments from file`);
      } catch (error) {
        console.log('No assignments file found, initializing with empty array');
        assignments = [];
      }
      
      try {
        const completedData = await fs.readFile(COMPLETED_FILE, 'utf8');
        completedAssignments = JSON.parse(completedData);
        console.log(`Loaded ${completedAssignments.length} completed assignments from file`);
      } catch (error) {
        console.log('No completed assignments file found, initializing with empty array');
        completedAssignments = [];
      }
      
      updateAgentAssignments();
    } else {
      // Just update agent assignments if not initial load
      updateAgentAssignments();
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
        originalRow: product.originalRow
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

async function saveProducts() {
  try {
    await fs.writeFile(PRODUCTS_FILE, JSON.stringify(products, null, 2));
    console.log('Products saved to file');
  } catch (error) {
    console.error('Error saving products:', error);
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

async function saveCompletedAssignments() {
  try {
    await fs.writeFile(COMPLETED_FILE, JSON.stringify(completedAssignments, null, 2));
    console.log('Completed assignments saved to file');
  } catch (error) {
    console.error('Error saving completed assignments:', error);
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
  res.json(completedAssignments);
});

// Endpoint to get manually unassigned products
app.get('/api/manually-unassigned', (req, res) => {
  const unassigned = products.filter(p => !p.assigned && p.wasAssigned);
  res.json(unassigned);
});

// Endpoint to get never assigned products (full queue)
app.get('/api/queue', (req, res) => {
  const queue = products.filter(p => !p.assigned && !p.wasAssigned);
  res.json(queue);
});

// Get statistics
app.get('/api/stats', (req, res) => {
  const stats = {
    totalAgents: agents.length,
    totalProducts: products.length,
    activeAssignments: assignments.length,
    completedAssignments: completedAssignments.length,
    unassignedProducts: products.filter(p => !p.assigned).length,
    manuallyUnassigned: products.filter(p => !p.assigned && p.wasAssigned).length,
    queuedProducts: products.filter(p => !p.assigned && !p.wasAssigned).length
  };
  res.json(stats);
});

// Upload a new CSV file to add products
app.post('/api/upload-output', upload.single('outputFile'), async (req, res) => {
  try {
    console.log('File upload received:', req.file);
    
    // Get the temporary path where multer stored the file
    const uploadedFilePath = req.file.path;
    
    // Merge the new products with existing ones
    const result = await mergeProductsFromCsv(uploadedFilePath);
    
    // Save the updated products list
    await saveProducts();
    
    // If you still want to keep the uploaded file as output.csv (optional)
    try {
      await fs.copyFile(uploadedFilePath, OUTPUT_CSV);
      console.log(`Copied uploaded file to ${OUTPUT_CSV}`);
    } catch (copyError) {
      console.warn(`Note: Could not save uploaded file as output.csv: ${copyError.message}`);
    }
    
    // Update agent assignments
    updateAgentAssignments();
    await saveAgents();
    
    // Send success response
    res.status(200).json({ 
      message: `CSV processed successfully. Added ${result.newProducts} new products and updated ${result.updatedProducts} existing products.`,
      newProducts: result.newProducts,
      updatedProducts: result.updatedProducts
    });
  } catch (error) {
    console.error('Error processing uploaded CSV:', error);
    res.status(500).json({ error: error.message });
  }
});

// Refresh endpoint
app.post('/api/refresh', async (req, res) => {
  try {
    // Load data without replacing existing data (false means not initial load)
    await loadData(false);
    console.log('Data refreshed successfully');
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
    
    // First try to assign products that were manually unassigned previously
    let availableProducts = products
      .filter(p => !p.assigned && !assignedProductIds.has(p.id) && p.wasAssigned)
      .sort((a, b) => {
        // Sort by unassigned time (oldest first)
        if (a.unassignedTime && b.unassignedTime) {
          return new Date(a.unassignedTime) - new Date(b.unassignedTime);
        }
        // Then by created date
        return new Date(a.createdOn) - new Date(b.createdOn);
      });
    
    // If no manually unassigned products, try to assign products that were never assigned
    if (availableProducts.length === 0) {
      availableProducts = products
        .filter(p => !p.assigned && !assignedProductIds.has(p.id) && !p.wasAssigned)
        .sort((a, b) => {
          // Sort by priority
          const priorityMap = { "P1": 0, "P2": 1, "P3": 2 };
          const aPriority = priorityMap[a.priority] || 999;
          const bPriority = priorityMap[b.priority] || 999;
          if (aPriority !== bPriority) {
            return aPriority - bPriority;
          }
          // Then by created date
          return new Date(a.createdOn) - new Date(b.createdOn);
        });
    }
    
    if (availableProducts.length === 0) {
      assignmentInProgress = false;
      return res.status(404).json({ error: 'No available products to assign' });
    }
    
    const productToAssign = availableProducts[0];
    
    // Update product status
    productToAssign.assigned = true;
    productToAssign.wasAssigned = true; // Mark that this product has been assigned before
    productToAssign.unassignedTime = null;
    productToAssign.unassignedBy = null;
    
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
    await saveProducts();
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
    
    // Get the assignment
    const assignment = assignments[assignmentIndex];
    
    // Mark as completed
    assignment.completed = true;
    assignment.completedOn = new Date().toISOString().replace('T', ' ').substring(0, 19);
    
    // Add to completed assignments
    completedAssignments.push(assignment);
    
    // Remove from active assignments
    assignments.splice(assignmentIndex, 1);
    
    // Update product
    const product = products.find(p => p.id === productId);
    if (product) {
      product.assigned = false;
      // Don't change wasAssigned, as it should remain true
    }
    
    // Update agent
    agent.currentAssignments = agent.currentAssignments.filter(task => task.productId !== productId);
    
    // Save changes
    await saveAssignments();
    await saveCompletedAssignments();
    await saveProducts();
    updateAgentAssignments();
    await saveAgents();
    
    res.status(200).json({ message: `Task ${productId} completed by ${agent.name}` });
  } catch (error) {
    console.error('Error completing task:', error);
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
    
    // Update agents
    for (const assignment of productAssignments) {
      const agent = agents.find(a => a.id === assignment.agentId);
      if (agent) {
        agent.currentAssignments = agent.currentAssignments.filter(task => task.productId !== productId);
      }
    }
    
    // Update product
    const product = products.find(p => p.id === productId);
    if (product) {
      product.assigned = false;
      product.wasAssigned = true; // Mark it as having been assigned previously
      product.unassignedTime = new Date().toISOString();
      product.unassignedBy = productAssignments[0].agentId; // Use first agent if multiple
    }
    
    // Remove assignments
    assignments = assignments.filter(a => a.productId !== productId || a.completed);
    
    // Save changes
    await saveAssignments();
    await saveProducts();
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
    
    // Update products
    const currentTime = new Date().toISOString();
    agent.currentAssignments.forEach(task => {
      const product = products.find(p => p.id === task.productId);
      if (product) {
        product.assigned = false;
        product.wasAssigned = true; // Mark it as having been assigned
        product.unassignedTime = currentTime;
        product.unassignedBy = agentId;
      }
    });
    
    // Remove assignments
    assignments = assignments.filter(a => a.agentId !== agentId || a.completed);
    
    // Clear agent assignments
    agent.currentAssignments = [];
    
    // Save changes
    await saveAssignments();
    await saveProducts();
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
    
    // Update products
    const currentTime = new Date().toISOString();
    for (const assignment of assignments.filter(a => !a.completed)) {
      const product = products.find(p => p.id === assignment.productId);
      if (product) {
        product.assigned = false;
        product.wasAssigned = true; // Mark it as having been assigned
        product.unassignedTime = currentTime;
        product.unassignedBy = 'all'; // Special value to indicate all agents
      }
    }
    
    // Clear assignments (but keep completed ones)
    assignments = assignments.filter(a => a.completed);
    
    // Clear agent assignments
    agents.forEach(a => { a.currentAssignments = []; });
    
    // Save changes
    await saveAssignments();
    await saveProducts();
    await saveAgents();
    
    res.status(200).json({ message: `Unassigned ${totalActive} tasks from all agents` });
  } catch (error) {
    console.error('Error unassigning all tasks:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Download completed assignments as CSV.
app.get('/api/download/completed-assignments', (req, res) => {
  res.setHeader('Content-disposition', 'attachment; filename=completed-tasks.csv');
  res.setHeader('Content-Type', 'text/csv');
  const csvStream = format({ headers: true });
  csvStream.pipe(res);
  completedAssignments.forEach(a => {
    const agent = agents.find(ag => ag.id === a.agentId);
    const product = products.find(p => p.id === a.productId);
    csvStream.write({
      assignmentId: a.id,
      agentName: agent ? agent.name : 'Unknown',
      productId: a.productId,
      assignedOn: a.assignedOn,
      completedOn: a.completedOn,
      priority: product ? product.priority : 'Unknown',
      tenantId: product ? product.tenantId : 'Unknown',
      count: product ? product.count : 0
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
      count: p.count,
      wasAssigned: p.wasAssigned ? 'Yes' : 'No',
      unassignedTime: p.unassignedTime || 'N/A'
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