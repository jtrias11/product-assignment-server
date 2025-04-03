/***************************************************************
 * Optimized Server Performance Script
 * Enhanced MongoDB Integration with Performance Improvements
 ***************************************************************/

// Error Handling
process.on('uncaughtException', (err) => {
  console.error('UNCAUGHT EXCEPTION:', err);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('UNHANDLED PROMISE REJECTION:', reason);
});

// Core Imports
require('dotenv').config();
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
const mongoose = require('mongoose');

// Model Imports
const Agent = require('./models/Agent');
const Product = require('./models/Product');
const Assignment = require('./models/Assignment');

// App Configuration
const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use((req, res, next) => {
  req.startTime = Date.now();
  next();
});

// Performance Logging Middleware
const performanceLogger = (req, res, next) => {
  res.on('finish', () => {
    const duration = Date.now() - req.startTime;
    console.log(`${req.method} ${req.path} - ${res.statusCode} (${duration}ms)`);
  });
  next();
};
app.use(performanceLogger);

// File Paths
const DATA_DIR = path.join(__dirname, 'data');
const OUTPUT_CSV = path.join(DATA_DIR, 'output.csv');
const ROSTER_EXCEL = path.join(DATA_DIR, 'Walmart BH Roster.xlsx');

// Multer Configuration
const storage = multer.diskStorage({
  destination: (req, file, cb) => { cb(null, DATA_DIR); },
  filename: (req, file, cb) => { 
    cb(null, `${Date.now()}-${file.originalname.replace(/\s+/g, '_')}`); 
  }
});
const upload = multer({ 
  storage,
  limits: { fileSize: 50 * 1024 * 1024 } // 50MB file size limit
});

// Cached Global Variables
let cachedAgents = [];
let cachedProducts = [];

// Enhanced MongoDB Connection
const connectDB = async () => {
  try {
    await mongoose.connect(process.env.MONGO_URI, {
      serverSelectionTimeoutMS: 30000,
      socketTimeoutMS: 45000,
      connectTimeoutMS: 45000,
      maxPoolSize: 20, // Connection pool optimization
      minPoolSize: 5
    });
    
    // Create indexes for performance
    await Promise.all([
      Product.createIndexes(),
      Agent.createIndexes(),
      Assignment.createIndexes()
    ]);
    
    console.log('MongoDB Connected with Performance Optimizations');
    return true;
  } catch (error) {
    console.error('MongoDB Connection Error:', error);
    return false;
  }
};

// Optimized Task Assignment Endpoint
let assignmentLock = false;
app.post('/api/assign', async (req, res) => {
  const startTime = Date.now();
  
  // Prevent concurrent assignments
  if (assignmentLock) {
    return res.status(409).json({ 
      error: 'Assignment in progress. Please try again.',
      processingTime: Date.now() - startTime 
    });
  }
  
  assignmentLock = true;
  
  try {
    const { agentId } = req.body;
    
    // Validate input
    if (!agentId) {
      assignmentLock = false;
      return res.status(400).json({ 
        error: 'Agent ID is required',
        processingTime: Date.now() - startTime 
      });
    }
    
    // Parallel optimized queries
    const [agent, assignedProductIds] = await Promise.all([
      Agent.findOne({ id: agentId }),
      Assignment.find({ 
        completed: false, 
        unassignedTime: { $exists: false } 
      }).distinct('productId')
    ]);
    
    // Agent validation
    if (!agent) {
      assignmentLock = false;
      return res.status(404).json({ 
        error: 'Agent not found',
        processingTime: Date.now() - startTime 
      });
    }
    
    // Capacity check
    if (agent.currentAssignments.length >= agent.capacity) {
      assignmentLock = false;
      return res.status(400).json({ 
        error: 'Agent has reached maximum capacity',
        processingTime: Date.now() - startTime 
      });
    }
    
    // Find available products with efficient query
    const availableProduct = await Product.findOne({
      assigned: false,
      id: { $nin: assignedProductIds }
    }).sort({ createdOn: 1 }); // Oldest first
    
    if (!availableProduct) {
      assignmentLock = false;
      return res.status(404).json({ 
        error: 'No available products',
        processingTime: Date.now() - startTime 
      });
    }
    
    // Create assignment with transaction for data integrity
    const session = await mongoose.startSession();
    try {
      await session.withTransaction(async () => {
        // Update product
        await Product.findByIdAndUpdate(
          availableProduct._id, 
          { assigned: true }, 
          { session }
        );
        
        // Create new assignment
        const newAssignment = new Assignment({
          id: uuidv4(),
          agentId: agent.id,
          productId: availableProduct.id,
          assignedOn: new Date().toISOString()
        });
        await newAssignment.save({ session });
      });
      
      assignmentLock = false;
      res.status(200).json({ 
        message: `Product ${availableProduct.id} assigned to agent`,
        processingTime: Date.now() - startTime 
      });
    } finally {
      session.endSession();
    }
  } catch (error) {
    assignmentLock = false;
    console.error('Assignment Error:', error);
    res.status(500).json({ 
      error: 'Server error during assignment',
      processingTime: Date.now() - startTime 
    });
  }
});

// Optimized CSV Upload Endpoint
app.post('/api/upload-output', upload.single('outputFile'), async (req, res) => {
  const startTime = Date.now();
  
  try {
    // Read CSV with streaming for large files
    const newData = await new Promise((resolve, reject) => {
      const results = [];
      createReadStream(req.file.path)
        .pipe(csvParser())
        .on('data', (row) => results.push(row))
        .on('end', () => resolve(results))
        .on('error', reject);
    });
    
    // Efficient bulk upsert with minimal database operations
    const bulkOps = newData.map(row => {
      const productId = row.abstract_product_id || 
                        row.item_abstract_product_id || 
                        row['item.abstract_product_id'] || 
                        row.product_id;
      
      return {
        updateOne: {
          filter: { id: productId },
          update: {
            $set: {
              id: productId,
              name: row.product_name || '',
              priority: row.rule_priority || row.priority || '',
              tenantId: row.tenant_id || '',
              createdOn: row.oldest_created_on || 
                         row.sys_created_on || 
                         row.created_on || 
                         new Date().toISOString(),
              count: row.count || 1,
              assigned: false
            }
          },
          upsert: true
        }
      };
    }).filter(op => op.updateOne.filter.id); // Ensure valid ID
    
    // Perform bulk write with error handling
    if (bulkOps.length > 0) {
      const result = await Product.bulkWrite(bulkOps, { 
        ordered: false,
        bypassDocumentValidation: true 
      });
      
      // Clean up temporary file
      await fs.unlink(req.file.path);
      
      res.status(200).json({ 
        message: 'CSV uploaded successfully',
        processed: bulkOps.length,
        upserted: result.upsertedCount,
        modified: result.modifiedCount,
        processingTime: Date.now() - startTime
      });
    } else {
      await fs.unlink(req.file.path);
      res.status(400).json({ 
        error: 'No valid products found in CSV',
        processingTime: Date.now() - startTime 
      });
    }
  } catch (error) {
    console.error('CSV Upload Error:', error);
    res.status(500).json({ 
      error: 'Server error during CSV upload',
      processingTime: Date.now() - startTime 
    });
  }
});

// Optimized Download Endpoints
app.get('/api/download/products', async (req, res) => {
  try {
    const products = await Product.find({}).lean();
    
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=products.csv');
    
    const csvStream = format({ headers: true });
    csvStream.pipe(res);
    
    products.forEach(product => {
      csvStream.write({
        id: product.id,
        name: product.name,
        priority: product.priority,
        tenantId: product.tenantId,
        createdOn: product.createdOn,
        count: product.count,
        assigned: product.assigned
      });
    });
    
    csvStream.end();
  } catch (error) {
    console.error('Product Download Error:', error);
    res.status(500).json({ error: 'Failed to download products' });
  }
});

// Cached Agents Endpoint
app.get('/api/agents', async (req, res) => {
  try {
    // Check if cached data exists and is recent
    if (cachedAgents.length > 0 && 
        Date.now() - (cachedAgents.lastUpdated || 0) < 5 * 60 * 1000) {
      return res.json(cachedAgents);
    }
    
    // Fetch fresh data
    const agents = await Agent.find({}).lean();
    
    // Cache the result
    cachedAgents = agents;
    cachedAgents.lastUpdated = Date.now();
    
    res.json(agents);
  } catch (error) {
    console.error('Agents Fetch Error:', error);
    res.status(500).json({ error: 'Failed to fetch agents' });
  }
});

// Server Startup
const startServer = async () => {
  try {
    await connectDB();
    
    const server = app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
    
    // Graceful shutdown
    process.on('SIGTERM', () => {
      console.log('SIGTERM signal received: closing HTTP server');
      server.close(() => {
        console.log('HTTP server closed');
        mongoose.connection.close(false, () => {
          console.log('MongoDB connection closed');
          process.exit(0);
        });
      });
    });
  } catch (error) {
    console.error('Server Startup Error:', error);
    process.exit(1);
  }
};

startServer();