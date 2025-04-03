console.log("Starting debug server...");

// Load environment variables
require('dotenv').config();
console.log("Loaded environment variables");

// Try loading each module separately
try {
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
  
  // Try loading model files
  console.log("Attempting to load model files...");
  const Agent = require('./models/Agent');
  console.log("Loaded Agent model");
  
  const Product = require('./models/Product');
  console.log("Loaded Product model");
  
  const Assignment = require('./models/Assignment');
  console.log("Loaded Assignment model");
  
  console.log("All modules loaded successfully!");
} catch (error) {
  console.error("Error loading modules:", error);
}