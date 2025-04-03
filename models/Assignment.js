const mongoose = require('mongoose');

const assignmentSchema = new mongoose.Schema({
  agentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Agent', required: true },
  productId: { type: String, required: true },
  assignedOn: { type: String },
  completed: { type: Boolean, default: false },
  completedOn: { type: String },
  unassignedTime: { type: String },
  unassignedBy: { type: String },
  wasUnassigned: { type: Boolean, default: false }
}, { timestamps: true });

module.exports = mongoose.model('Assignment', assignmentSchema);
