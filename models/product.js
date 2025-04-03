const mongoose = require('mongoose');

const AssignmentSchema = new mongoose.Schema({
  id: String,
  agentId: Number,
  productId: String,
  assignedOn: String,
  completed: { type: Boolean, default: false },
  completedOn: String,
  unassignedTime: String,
  unassignedBy: String,
  wasUnassigned: { type: Boolean, default: false }
}, { timestamps: true });

module.exports = mongoose.model('Assignment', AssignmentSchema);