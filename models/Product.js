const mongoose = require('mongoose');

const productSchema = new mongoose.Schema({
  id: { type: String, required: true, unique: true },
  name: { type: String, default: "Unnamed Product" },
  priority: { type: String },
  tenantId: { type: String },
  createdOn: { type: String }, // Alternatively, you could use Date if you convert the CSV strings
  count: { type: Number, default: 1 },
  assigned: { type: Boolean, default: false }
}, { timestamps: true });

module.exports = mongoose.model('Product', productSchema);
