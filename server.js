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

// Updated readCsvFiles function that processes all CSV files concurrently
async function readCsvFiles() {
  try {
    const files = await fs.readdir(DATA_DIR);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));

    if (csvFiles.length === 0) {
      console.log('No CSV files found in data directory');
      return [];
    }

    // Process all CSV files concurrently.
    const results = await Promise.all(csvFiles.map(file => {
      return new Promise((resolve, reject) => {
        const filePath = path.join(DATA_DIR, file);
        const fileProducts = [];
        let rowCount = 0;
        
        createReadStream(filePath)
          .pipe(csv())
          .on('data', (data) => {
            rowCount++;
            // Use the correct field name for the abstract id.
            let productId = data['item.abstract_product_id'] || null;
            if (!productId || productId.trim() === '') {
              return; // Skip rows with blank abstract id.
            }
            // Build a product object.
            const product = {
              id: productId,
              itemId: parseInt(data.ItemID || data.item_id || 0),
              name: data.ItemName || data.Name || data.name || data.Description || 'Unknown Product',
              priority: data['rule.priority'] || data.priority || 'P3',
              createdOn: data.sys_created_on || data.created_on || data.CreatedOn ||
                         new Date().toISOString().replace('T', ' ').substring(0, 19),
              tenantId: data.tenant_id || data.TenantID || data['Tenant ID'] || '',
              assigned: false
            };
            fileProducts.push(product);
          })
          .on('end', () => {
            console.log(`File ${file} processed: ${fileProducts.length} valid rows (out of ${rowCount} rows)`);
            resolve(fileProducts);
          })
          .on('error', (error) => {
            console.error(`Error reading CSV file ${file}:`, error);
            resolve([]); // Resolve with an empty array on error.
          });
      });
    }));

    // Combine results from all files.
    let allProducts = results.flat();

    // Aggregate products by unique abstract id.
    const abstractIdCounts = {};
    for (const product of allProducts) {
      abstractIdCounts[product.id] = (abstractIdCounts[product.id] || 0) + 1;
    }
    // Keep only one product per unique id and attach the count.
    const uniqueProducts = {};
    for (const product of allProducts) {
      if (!uniqueProducts[product.id]) {
        uniqueProducts[product.id] = product;
        uniqueProducts[product.id].count = abstractIdCounts[product.id];
      }
    }
    const finalProducts = Object.values(uniqueProducts);
    console.log(`Total unique products loaded from CSVs: ${finalProducts.length}`);
    return finalProducts;
  } catch (error) {
    console.error('Error reading CSV files:', error);
    return [];
  }
}

// Function to read Excel roster file (agents)
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
      console.log('Sheet "Agents List" not found, checking alternative names...');
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
    // Loop through rows (skip header row) and extract names from column E (index 4)
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

// Helper function to check if a file exists
async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

// Load data from files or initialize with sample data
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
        console.log('Excel import failed, initializing with sample agent data');
        agents = [
          { id: 1, name: "Aaron Dale Yaeso Bandong", role: "Item Review", capacity: 10, currentAssignments: [] },
          { id: 2, name: "Aaron Marx Lenin Tuban Oriola", role: "Item Review", capacity: 10, currentAssignments: [] }
        ];
        await saveAgents();
      }
    }
    
    // Load products
    try {
      const productsData = await fs.readFile(PRODUCTS_FILE, 'utf8');
      products = JSON.parse(productsData);
      console.log(`Loaded ${products.length} products from JSON file`);
    } catch (error) {
      console.log('No products JSON file found, importing from CSV files');
      const csvProducts = await readCsvFiles();
      if (csvProducts.length > 0) {
        products = csvProducts;
        await saveProducts();
      } else {
        console.log('CSV import failed, initializing with sample product data');
        products = [];
        for (let i = 0; i < 20; i++) {
          const priority = i % 3 === 0 ? "P1" : (i % 3 === 1 ? "P2" : "P3");
          const itemId = 15847610000 + i;
          products.push({
            id: `SAMPLE${i.toString().padStart(5, '0')}`,
            itemId,
            name: `Sample Product ${i + 1} - ${["Sweater", "Jeans", "T-Shirt", "Jacket", "Dress"][i % 5]} Item`,
            priority,
            tenantId: `TENANT${(i % 3) + 1}`,
            createdOn: new Date().toISOString().replace('T', ' ').substring(0, 19),
            assigned: false,
            count: Math.floor(Math.random() * 5) + 1,
            unassignedTime: null
          });
        }
        await saveProducts();
      }
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

// Save functions
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

// API Routes
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
        if (a.unassignedTime && !b.unassignedTime) return -1;
        if (!a.unassignedTime && b.unassignedTime) return 1;
        if (a.unassignedTime && b.unassignedTime) {
          return new Date(a.unassignedTime) - new Date(b.unassignedTime);
        }
        const priorityDiff = priorityOrder[a.priority] - priorityOrder[b.priority];
        if (priorityDiff !== 0) return priorityDiff;
        return new Date(a.createdOn) - new Date(b.createdOn);
      });
    
    if (availableProducts.length === 0) {
      assignmentInProgress = false;
      return res.status(404).json({ error: 'No available products to assign' });
    }
    
    const productToAssign = availableProducts[0];
    productToAssign.unassignedTime = null;
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
    
    const sameAbstractIdProducts = products.filter(p => 
      p.id === productToAssign.id && p !== productToAssign
    );
    sameAbstractIdProducts.forEach(p => {
      p.assigned = true;
      p.unassignedTime = null;
    });
    
    console.log(`Assigned product ID ${productToAssign.id} to agent ${agent.name}`);
    
    await saveProducts();
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

app.post('/api/complete', async (req, res) => {
  try {
    const { agentId, productId } = req.body;
    if (!agentId || !productId) {
      return res.status(400).json({ error: 'Agent ID and Product ID are required' });
    }
    const agent = agents.find(a => a.id === agentId);
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }
    const assignmentIndex = assignments.findIndex(
      a => a.agentId === agentId && a.productId === productId
    );
    if (assignmentIndex === -1) {
      return res.status(404).json({ error: 'Assignment not found' });
    }
    assignments.splice(assignmentIndex, 1);
    agent.currentAssignments = agent.currentAssignments.filter(
      task => task.productId !== productId
    );
    const productsWithThisId = products.filter(p => p.id === productId);
    productsWithThisId.forEach(product => {
      product.assigned = false;
      product.completed = true;
    });
    console.log(`Completed product ID ${productId} by agent ${agent.name}`);
    await saveProducts();
    await saveAssignments();
    await saveAgents();
    res.status(200).json({ message: `Task ${productId} completed by ${agent.name}` });
  } catch (error) {
    console.error('Error completing task:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

app.post('/api/unassign-all', async (req, res) => {
  try {
    const currentAssignmentCount = assignments.length;
    const currentTime = new Date().toISOString();
    assignments.forEach(assignment => {
      const product = products.find(p => p.id === assignment.productId);
      if (product) {
        product.assigned = false;
        product.unassignedTime = currentTime;
      }
    });
    assignments = [];
    agents.forEach(agent => {
      agent.currentAssignments = [];
    });
    await saveProducts();
    await saveAssignments();
    await saveAgents();
    console.log(`Unassigned all ${currentAssignmentCount} tasks`);
    res.status(200).json({ 
      message: `Successfully unassigned all ${currentAssignmentCount} tasks. They will be prioritized for future assignments.`
    });
  } catch (error) {
    console.error('Error unassigning all tasks:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

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
    const agentAssignmentCount = agent.currentAssignments.length;
    if (agentAssignmentCount === 0) {
      return res.status(200).json({ message: 'Agent has no tasks to unassign' });
    }
    const currentTime = new Date().toISOString();
    const productIds = agent.currentAssignments.map(task => task.productId);
    productIds.forEach(productId => {
      const productsWithThisId = products.filter(p => p.id === productId);
      productsWithThisId.forEach(product => {
        product.assigned = false;
        product.unassignedTime = currentTime;
      });
    });
    assignments = assignments.filter(a => a.agentId !== agentId);
    agent.currentAssignments = [];
    await saveProducts();
    await saveAssignments();
    await saveAgents();
    console.log(`Unassigned ${agentAssignmentCount} tasks from agent ${agent.name}`);
    res.status(200).json({ 
      message: `Successfully unassigned ${agentAssignmentCount} tasks from ${agent.name}. They will be prioritized for future assignments.`
    });
  } catch (error) {
    console.error('Error unassigning agent tasks:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

app.post('/api/unassign-product', async (req, res) => {
  try {
    const { productId } = req.body;
    if (!productId) {
      return res.status(400).json({ error: 'Product ID is required' });
    }
    const productAssignments = assignments.filter(a => a.productId === productId);
    if (productAssignments.length === 0) {
      return res.status(404).json({ error: 'Product is not currently assigned to any agent' });
    }
    const currentTime = new Date().toISOString();
    const affectedAgentIds = productAssignments.map(a => a.agentId);
    affectedAgentIds.forEach(agentId => {
      const agent = agents.find(a => a.id === agentId);
      if (agent) {
        agent.currentAssignments = agent.currentAssignments.filter(
          task => task.productId !== productId
        );
      }
    });
    const productsWithThisId = products.filter(p => p.id === productId);
    productsWithThisId.forEach(product => {
      product.assigned = false;
      product.unassignedTime = currentTime;
    });
    assignments = assignments.filter(a => a.productId !== productId);
    await saveProducts();
    await saveAssignments();
    await saveAgents();
    const affectedAgents = affectedAgentIds.length === 1 ? 
      `agent ${agents.find(a => a.id === affectedAgentIds[0]).name}` : 
      `${affectedAgentIds.length} agents`;
    console.log(`Unassigned product ID ${productId} from ${affectedAgents}`);
    res.status(200).json({ 
      message: `Successfully unassigned product ID ${productId} from ${affectedAgents}. It will be prioritized for future assignments.`
    });
  } catch (error) {
    console.error('Error unassigning product:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Refresh endpoint to re-read data from files
app.post('/api/refresh', async (req, res) => {
  try {
    await loadData();
    console.log('Data refreshed successfully');
    res.status(200).json({ message: 'Data refreshed successfully' });
  } catch (error) {
    console.error('Error refreshing data:', error);
    res.status(500).json({ error: 'Failed to refresh data' });
  }
});

// Start the server
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await loadData();
  console.log('Server is ready to handle requests');
});
