const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs').promises;
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3001;

// Enable CORS for all routes
app.use(cors());
app.use(express.json());

// Data storage
let agents = [];
let products = [];
let assignments = [];

// Simple in-memory data persistence
const DATA_DIR = path.join(__dirname, 'data');
const AGENTS_FILE = path.join(DATA_DIR, 'agents.json');
const PRODUCTS_FILE = path.join(DATA_DIR, 'products.json');
const ASSIGNMENTS_FILE = path.join(DATA_DIR, 'assignments.json');

// Ensure data directory exists
async function ensureDataDir() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });
    console.log('Data directory is ready');
  } catch (error) {
    console.error('Error creating data directory:', error);
  }
}

// Load data from files or initialize with sample data
async function loadData() {
  try {
    await ensureDataDir();
    
    try {
      const agentsData = await fs.readFile(AGENTS_FILE, 'utf8');
      agents = JSON.parse(agentsData);
      console.log(`Loaded ${agents.length} agents from file`);
    } catch (error) {
      console.log('No agents file found or error loading, initializing with sample data');
      // Sample agents
      agents = [
        { id: 1, name: "Aaron Dale Yaeso Bandong", role: "Item Review", capacity: 10, currentAssignments: [] },
        { id: 2, name: "Aaron Marx Lenin Tuban Oriola", role: "Item Review", capacity: 10, currentAssignments: [] },
        { id: 3, name: "Abel Alicaya Cabugnason", role: "Item Review", capacity: 10, currentAssignments: [] },
        { id: 4, name: "Adam Paul Medina Baliguat", role: "Item Review", capacity: 10, currentAssignments: [] },
        { id: 5, name: "Aileen Punsalan Dionisio", role: "Item Review", capacity: 10, currentAssignments: [] },
        { id: 6, name: "Aileen Sandoval Galicia", role: "Item Review", capacity: 10, currentAssignments: [] },
        { id: 7, name: "Albert Corpuz Pucyutan", role: "Item Review", capacity: 10, currentAssignments: [] },
        { id: 8, name: "Albert Mahinay Saligumba", role: "Item Review", capacity: 10, currentAssignments: [] },
        { id: 9, name: "Aldwin De Vega Morales", role: "Item Review", capacity: 10, currentAssignments: [] },
        { id: 10, name: "Alexander Panganiban De Leon", role: "Item Review", capacity: 10, currentAssignments: [] },
        { id: 11, name: "Alexia Muncada Uy", role: "Item Review", capacity: 10, currentAssignments: [] },
        { id: 12, name: "Allyson Romero Montesclaros", role: "Item Review", capacity: 10, currentAssignments: [] }
      ];
      await saveAgents();
    }
    
    try {
      const productsData = await fs.readFile(PRODUCTS_FILE, 'utf8');
      products = JSON.parse(productsData);
      console.log(`Loaded ${products.length} products from file`);
    } catch (error) {
      console.log('No products file found or error loading, initializing with sample data');
      // Generate 20 sample products
      products = [];
      for (let i = 0; i < 20; i++) {
        const priority = i % 3 === 0 ? "P1" : (i % 3 === 1 ? "P2" : "P3");
        const itemId = 15847610000 + i;
        products.push({
          id: uuidv4().substring(0, 12).toUpperCase(),
          itemId,
          name: `Sample Product ${i+1} - ${["Sweater", "Jeans", "T-Shirt", "Jacket", "Dress"][i % 5]} Item`,
          priority,
          createdOn: new Date().toISOString().replace('T', ' ').substring(0, 19),
          assigned: false
        });
      }
      await saveProducts();
    }
    
    try {
      const assignmentsData = await fs.readFile(ASSIGNMENTS_FILE, 'utf8');
      assignments = JSON.parse(assignmentsData);
      console.log(`Loaded ${assignments.length} assignments from file`);
      
      // Update agent currentAssignments based on loaded assignments
      updateAgentAssignments();
    } catch (error) {
      console.log('No assignments file found or error loading, initializing with empty array');
      assignments = [];
      await saveAssignments();
    }
  } catch (error) {
    console.error('Error loading data:', error);
  }
}

// Update agents' currentAssignments based on assignments array
function updateAgentAssignments() {
  // Reset all agents' currentAssignments
  agents.forEach(agent => {
    agent.currentAssignments = [];
  });
  
  // Update based on assignments
  assignments.forEach(assignment => {
    const agent = agents.find(a => a.id === assignment.agentId);
    const product = products.find(p => p.id === assignment.productId);
    
    if (agent && product) {
      agent.currentAssignments.push({
        productId: product.id,
        name: product.name,
        priority: product.priority
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
// Root route for health check
app.get('/', (req, res) => {
  res.send('Product Assignment Server is running');
});

// Get all agents
app.get('/api/agents', (req, res) => {
  res.json(agents);
});

// Get all products
app.get('/api/products', (req, res) => {
  res.json(products);
});

// Get all assignments
app.get('/api/assignments', (req, res) => {
  res.json(assignments);
});

// Assign a task to an agent
app.post('/api/assign', async (req, res) => {
  try {
    const { agentId } = req.body;
    
    if (!agentId) {
      return res.status(400).json({ error: 'Agent ID is required' });
    }
    
    const agent = agents.find(a => a.id === agentId);
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }
    
    if (agent.currentAssignments.length >= agent.capacity) {
      return res.status(400).json({ error: 'Agent has reached maximum capacity' });
    }
    
    // Find an unassigned product with highest priority
    const priorityOrder = { "P1": 0, "P2": 1, "P3": 2 };
    const availableProducts = products
      .filter(p => !p.assigned)
      .sort((a, b) => {
        // First sort by priority
        const priorityDiff = priorityOrder[a.priority] - priorityOrder[b.priority];
        if (priorityDiff !== 0) return priorityDiff;
        
        // Then sort by creation date (oldest first)
        return new Date(a.createdOn) - new Date(b.createdOn);
      });
    
    if (availableProducts.length === 0) {
      return res.status(404).json({ error: 'No available products to assign' });
    }
    
    const productToAssign = availableProducts[0];
    
    // Update product status
    productToAssign.assigned = true;
    
    // Create assignment
    const assignment = {
      id: uuidv4(),
      agentId: agent.id,
      productId: productToAssign.id,
      assignedOn: new Date().toISOString().replace('T', ' ').substring(0, 19)
    };
    
    assignments.push(assignment);
    
    // Update agent's current assignments
    agent.currentAssignments.push({
      productId: productToAssign.id,
      name: productToAssign.name,
      priority: productToAssign.priority
    });
    
    // Save changes
    await saveProducts();
    await saveAssignments();
    await saveAgents();
    
    res.status(200).json({ 
      message: `Task ${productToAssign.id} assigned to ${agent.name}`,
      assignment
    });
  } catch (error) {
    console.error('Error assigning task:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Complete a task
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
    
    // Remove the assignment
    assignments.splice(assignmentIndex, 1);
    
    // Update agent's current assignments
    agent.currentAssignments = agent.currentAssignments.filter(
      task => task.productId !== productId
    );
    
    // Update product (mark as completed, we'll keep it in the system but no longer assigned)
    const product = products.find(p => p.id === productId);
    if (product) {
      product.assigned = false;
      product.completed = true;
    }
    
    // Save changes
    await saveProducts();
    await saveAssignments();
    await saveAgents();
    
    res.status(200).json({ 
      message: `Task ${productId} completed by ${agent.name}`,
    });
  } catch (error) {
    console.error('Error completing task:', error);
    res.status(500).json({ error: `Server error: ${error.message}` });
  }
});

// Start the server
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await loadData();
  console.log('Server is ready to handle requests');
});