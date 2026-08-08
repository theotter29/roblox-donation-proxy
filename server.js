require('dotenv').config();
const express = require('express');
const cors = require('cors');
const app = express();

app.use(cors());
app.use(express.json());

// Simpan donation data (temporary - di memory)
let lastDonation = null;

// Saweria webhook endpoint
app.post('/webhook/saweria', (req, res) => {
  try {
    const donation = {
      name: req.body.supporter_name || 'Anonymous',
      amount: req.body.amount || 0,
      message: req.body.message || '',
      timestamp: new Date().toISOString()
    };
    
    lastDonation = donation;
    console.log('✅ Donation received:', donation);
    
    res.json({ success: true, message: 'Donation logged' });
  } catch (error) {
    console.error('❌ Error:', error);
    res.status(400).json({ success: false, error: error.message });
  }
});

// Roblox polling endpoint (ambil data donation terakhir)
app.get('/api/donation', (req, res) => {
  res.json({ donation: lastDonation });
});

// Health check
app.get('/', (req, res) => {
  res.json({ status: 'Server running ✅' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});