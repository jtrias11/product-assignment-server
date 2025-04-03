const mongoose = require('mongoose');

const agentSchema = new mongoose.Schema({
  name: { type: String, required: true },
  role: { type: String, default: 'Item Review' },
  capacity: { type: Number, default: 30 }
}, { timestamps: true });

module.exports = mongoose.model('Agent', agentSchema);
