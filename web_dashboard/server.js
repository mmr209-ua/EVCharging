// server.js - Front Web para EVCharging Release 2
// Servidor Express que sirve dashboard y hace proxy a API_Central

const express = require('express');
const axios = require('axios');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const API_CENTRAL_URL = process.env.API_CENTRAL_URL || 'http://localhost:5002';

// Servir archivos estaticos
app.use(express.static(path.join(__dirname, 'public')));

// ======================================================================
// PROXY ENDPOINTS - Evita problemas de CORS en navegadores
// ======================================================================

// Proxy para obtener CPs
app.get('/api/cps', async (req, res) => {
    try {
        const response = await axios.get(`${API_CENTRAL_URL}/cps`);
        res.json(response.data);
    } catch (error) {
        console.error('[WEB] Error obteniendo CPs:', error.message);
        res.status(500).json({ error: true, message: 'Error conectando con API_Central' });
    }
});

// Proxy para obtener conductores
app.get('/api/drivers', async (req, res) => {
    try {
        const response = await axios.get(`${API_CENTRAL_URL}/drivers`);
        res.json(response.data);
    } catch (error) {
        console.error('[WEB] Error obteniendo conductores:', error.message);
        res.status(500).json({ error: true, message: 'Error conectando con API_Central' });
    }
});

// Proxy para obtener transacciones
app.get('/api/transactions', async (req, res) => {
    try {
        const limit = req.query.limit || 50;
        const response = await axios.get(`${API_CENTRAL_URL}/transactions?limit=${limit}`);
        res.json(response.data);
    } catch (error) {
        console.error('[WEB] Error obteniendo transacciones:', error.message);
        res.status(500).json({ error: true, message: 'Error conectando con API_Central' });
    }
});

// Proxy para obtener auditoria
app.get('/api/audit', async (req, res) => {
    try {
        const limit = req.query.limit || 100;
        const response = await axios.get(`${API_CENTRAL_URL}/audit?limit=${limit}`);
        res.json(response.data);
    } catch (error) {
        console.error('[WEB] Error obteniendo auditoria:', error.message);
        res.status(500).json({ error: true, message: 'Error conectando con API_Central' });
    }
});

// Proxy para obtener estado del clima
app.get('/api/weather', async (req, res) => {
    try {
        const response = await axios.get(`${API_CENTRAL_URL}/weather`);
        res.json(response.data);
    } catch (error) {
        console.error('[WEB] Error obteniendo clima:', error.message);
        res.status(500).json({ error: true, message: 'Error conectando con API_Central' });
    }
});

// Health check
app.get('/api/health', async (req, res) => {
    try {
        const response = await axios.get(`${API_CENTRAL_URL}/health`);
        res.json({
            web: 'ok',
            api_central: response.data.status
        });
    } catch (error) {
        res.json({
            web: 'ok',
            api_central: 'unreachable'
        });
    }
});

// Ruta principal - servir index.html
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Iniciar servidor
app.listen(PORT, () => {
    console.log('[WEB] ==========================================');
    console.log('[WEB] EVCharging Dashboard - Release 2');
    console.log('[WEB] ==========================================');
    console.log(`[WEB] Servidor web: http://localhost:${PORT}`);
    console.log(`[WEB] API Central:  ${API_CENTRAL_URL}`);
    console.log('[WEB] ==========================================');
});
