console.log("Starting full debug server...");
require('dotenv').config();
console.log("Loaded environment variables");

// Load all required modules
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
console.log("Loaded all required modules");

// Load models
const Agent = require('./models/Agent');
const Product = require('./models/Product');
const Assignment = require('./models/Assignment');
console.log("Loaded all models");

// Create Express app
const app = express();
const PORT = process.env.PORT || 3001;
console.log(`Server will run on port ${PORT}`);

app.use(cors());
app.use(express.json());
console.log("Set up Express middleware");

// MongoDB Connection
const connectDB = async () => {
  console.log("Attempting to connect to MongoDB...");
  try {
    await mongoose.connect(process.env.MONGO_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true
    });
    console.log('MongoDB Connected!');
    return true;
  } catch (error) {
    console.error('MongoDB Connection Error:', error);
    return false;
  }
};

// Define a basic route
app.get('/', (req, res) => {
  res.send('Debug server is running');
});

// Start server
console.log("Starting Express server...");
const server = app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  
  console.log("Now connecting to MongoDB...");
  try {
    const connected = await connectDB();
    console.log(`MongoDB connection success: ${connected}`);
  } catch (error) {
    console.error("Error during MongoDB connection:", error);
  }
  
  console.log("Server startup complete");
});

// Add error handler for the server
server.on('error', (error) => {
  console.error('Server error:', error);
});