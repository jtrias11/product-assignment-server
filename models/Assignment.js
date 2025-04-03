const mongoose = require('mongoose');

const ProductSchema = new mongoose.Schema({
  id: String,
  name: String,
  priority: String,
  tenantId: String,
  createdOn: String,
  count: { type: Number, default: 1 },
  assigned: { type: Boolean, default: false }
}, { timestamps: true });

module.exports = mongoose.model('Product', ProductSchema);