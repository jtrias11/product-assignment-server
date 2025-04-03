const mongoose = require('mongoose');

const AgentSchema = new mongoose.Schema({
  id: Number,
  name: String,
  role: String,
  capacity: { type: Number, default: 10 },
  currentAssignments: { type: Array, default: [] }
}, { timestamps: true });

module.exports = mongoose.model('Agent', AgentSchema);