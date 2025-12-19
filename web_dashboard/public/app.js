// app.js - Frontend JavaScript para EVCharging Dashboard

const API_BASE = '/api';
const REFRESH_INTERVAL = 5000; // 5 segundos

// Estado de la aplicacion
let isConnected = false;

// ======================================================================
// INICIALIZACION
// ======================================================================

document.addEventListener('DOMContentLoaded', () => {
    setupNavigation();
    startDataRefresh();
    updateAll();
});

function setupNavigation() {
    const navButtons = document.querySelectorAll('.nav-btn');
    navButtons.forEach(btn => {
        btn.addEventListener('click', () => {
            // Desactivar todos los botones y tabs
            navButtons.forEach(b => b.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));

            // Activar el boton y tab seleccionado
            btn.classList.add('active');
            const tabId = btn.dataset.tab + '-tab';
            document.getElementById(tabId).classList.add('active');
        });
    });
}

function startDataRefresh() {
    setInterval(updateAll, REFRESH_INTERVAL);
}

async function updateAll() {
    try {
        await Promise.all([
            fetchCPs(),
            fetchDrivers(),
            fetchTransactions(),
            fetchWeather(),
            fetchAudit()
        ]);
        setConnectionStatus(true);
        updateLastRefresh();
    } catch (error) {
        console.error('Error actualizando datos:', error);
        setConnectionStatus(false);
    }
}

// ======================================================================
// ESTADO DE CONEXION
// ======================================================================

function setConnectionStatus(connected) {
    isConnected = connected;
    const statusEl = document.getElementById('connection-status');
    if (connected) {
        statusEl.textContent = 'Conectado';
        statusEl.className = 'status-indicator connected';
    } else {
        statusEl.textContent = 'Desconectado';
        statusEl.className = 'status-indicator disconnected';
    }
}

function updateLastRefresh() {
    const now = new Date();
    const timeStr = now.toLocaleTimeString('es-ES');
    document.getElementById('last-update').textContent = `Ultima actualizacion: ${timeStr}`;
}

// ======================================================================
// CHARGING POINTS
// ======================================================================

async function fetchCPs() {
    try {
        const response = await fetch(`${API_BASE}/cps`);
        if (!response.ok) throw new Error('Error fetching CPs');
        const data = await response.json();
        renderCPs(data);
    } catch (error) {
        console.error('Error fetching CPs:', error);
        throw error;
    }
}

function renderCPs(cps) {
    // Actualizar contadores
    const total = cps.length;
    const activos = cps.filter(cp => cp.estado === 'ACTIVADO').length;
    const suministrando = cps.filter(cp => cp.estado === 'SUMINISTRANDO').length;
    const pausados = cps.filter(cp => cp.paused_by_weather).length;

    document.getElementById('total-cps').textContent = total;
    document.getElementById('active-cps').textContent = activos;
    document.getElementById('charging-cps').textContent = suministrando;
    document.getElementById('paused-cps').textContent = pausados;

    // Actualizar selector de CPs
    const cpSelect = document.getElementById('cp-select');
    const currentValue = cpSelect.value;
    cpSelect.innerHTML = '<option value="">-- Seleccionar --</option>' +
        cps.map(cp => `<option value="${cp.idCP}">CP ${cp.idCP} (${cp.estado})</option>`).join('');
    cpSelect.value = currentValue;

    // Renderizar tabla
    const tbody = document.querySelector('#cps-table tbody');

    if (cps.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="no-data">No hay CPs registrados</td></tr>';
        return;
    }

    tbody.innerHTML = cps.map(cp => {
        const estadoBadge = getEstadoBadge(cp.estado);
        const authBadge = cp.authenticated
            ? '<span class="badge badge-green">SI</span>'
            : '<span class="badge badge-gray">NO</span>';
        const climaBadge = cp.paused_by_weather
            ? '<span class="badge badge-blue">FUERA DE SERVICIO</span>'
            : '<span class="badge badge-green">OK</span>';
        const token = cp.authToken || cp.auth_token || 'N/A';
        const tokenDisplay = token !== 'N/A' && token.length > 12
            ? token.substring(0, 12) + '...'
            : token;

        return `
            <tr>
                <td><strong>${cp.idCP}</strong></td>
                <td>${estadoBadge}</td>
                <td>${cp.precio ? cp.precio.toFixed(2) : 'N/A'}</td>
                <td>${cp.ubicacion || 'N/A'}</td>
                <td>${authBadge}</td>
                <td><code title="${token}">${tokenDisplay}</code></td>
                <td>${climaBadge}</td>
            </tr>
        `;
    }).join('');
}

function getEstadoBadge(estado) {
    const badges = {
        'ACTIVADO': '<span class="badge badge-green">ACTIVADO</span>',
        'SUMINISTRANDO': '<span class="badge badge-blue">SUMINISTRANDO</span>',
        'PARADO': '<span class="badge badge-orange">PARADO</span>',
        'AVERIADO': '<span class="badge badge-red">AVERIADO</span>',
        'DESACTIVADO': '<span class="badge badge-gray">DESACTIVADO</span>'
    };
    return badges[estado] || `<span class="badge badge-gray">${estado}</span>`;
}

// ======================================================================
// CONDUCTORES
// ======================================================================

let cachedTransactions = [];

async function fetchDrivers() {
    try {
        const [driversRes, transactionsRes] = await Promise.all([
            fetch(`${API_BASE}/drivers`),
            fetch(`${API_BASE}/transactions?limit=1000`)
        ]);

        if (!driversRes.ok) throw new Error('Error fetching drivers');
        if (!transactionsRes.ok) throw new Error('Error fetching transactions');

        const drivers = await driversRes.json();
        const txData = await transactionsRes.json();
        cachedTransactions = txData.transactions;
        renderDrivers(drivers, cachedTransactions, txData.total_drivers);
    } catch (error) {
        console.error('Error fetching drivers:', error);
        throw error;
    }
}

function renderDrivers(drivers, transactions, numDrivers) {
    // Actualizar contadores globales
    const totalDrivers = numDrivers ?? drivers.length;
    const totalRecargas = transactions.length;
    const totalConsumo = transactions.reduce((sum, tx) => sum + (tx.consumo || 0), 0);
    const totalImporte = transactions.reduce((sum, tx) => sum + (tx.importe || 0), 0);

    document.getElementById('total-drivers').textContent = totalDrivers;
    document.getElementById('total-recargas').textContent = totalRecargas;
    document.getElementById('total-consumo').textContent = totalConsumo.toFixed(2);
    document.getElementById('total-importe').textContent = totalImporte.toFixed(2);
}

// ======================================================================
// TRANSACCIONES
// ======================================================================

async function fetchTransactions() {
    try {
        const response = await fetch(`${API_BASE}/transactions?limit=50`);
        if (!response.ok) throw new Error('Error fetching transactions');
        const data = await response.json();
        renderTransactions(data.transactions);
    } catch (error) {
        console.error('Error fetching transactions:', error);
        throw error;
    }
}

function renderTransactions(transactions) {
    const tbody = document.querySelector('#transactions-table tbody');

    if (transactions.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="no-data">No hay transacciones</td></tr>';
        return;
    }

    tbody.innerHTML = transactions.map(tx => {
        const estadoBadge = getTransactionBadge(tx.estado);
        const consumo = tx.consumo ? tx.consumo.toFixed(2) : '0.00';
        const importe = tx.importe ? tx.importe.toFixed(2) : '0.00';

        return `
            <tr>
                <td>${tx.idConsumo}</td>
                <td>${tx.conductor}</td>
                <td>${tx.cp}</td>
                <td>${estadoBadge}</td>
                <td>${consumo}</td>
                <td>${importe}</td>
                <td>${tx.timestamp || 'N/A'}</td>
            </tr>
        `;
    }).join('');
}

function getTransactionBadge(estado) {
    const badges = {
        'COMPLETADO': '<span class="badge badge-green">COMPLETADO</span>',
        'EN_PROGRESO': '<span class="badge badge-blue">EN PROGRESO</span>',
        'CANCELADO': '<span class="badge badge-orange">CANCELADO</span>',
        'ERROR': '<span class="badge badge-red">ERROR</span>'
    };
    return badges[estado] || `<span class="badge badge-gray">${estado}</span>`;
}

// ======================================================================
// CLIMA
// ======================================================================

async function fetchWeather() {
    try {
        const response = await fetch(`${API_BASE}/weather`);
        if (!response.ok) throw new Error('Error fetching weather');
        const data = await response.json();
        renderWeather(data);
    } catch (error) {
        console.error('Error fetching weather:', error);
        throw error;
    }
}

function renderWeather(weatherData) {
    const container = document.getElementById('weather-cards');

    if (weatherData.length === 0) {
        container.innerHTML = '<div class="no-data">No hay datos de clima disponibles</div>';
        return;
    }

    container.innerHTML = weatherData.map(w => {
        const temp = w.temperatura !== null ? w.temperatura : '--';
        const isAlert = w.alert_active;
        const tempClass = getTempClass(temp);

        return `
            <div class="weather-card ${isAlert ? 'alert-active' : ''}">
                <div class="weather-location">${w.ubicacion}</div>
                <div class="weather-temp ${tempClass}">${temp}°C</div>
                <div class="weather-status">
                    Actualizado: ${w.timestamp || 'N/A'}
                </div>
                ${isAlert ? '<div class="weather-alert">ALERTA: Temperatura bajo cero</div>' : ''}
            </div>
        `;
    }).join('');
}

function getTempClass(temp) {
    if (temp === '--') return '';
    if (temp < 0) return 'freezing';
    if (temp < 10) return 'cold';
    return 'warm';
}

// ======================================================================
// AUDITORIA
// ======================================================================

async function fetchAudit() {
    try {
        const response = await fetch(`${API_BASE}/audit?limit=100`);
        if (!response.ok) throw new Error('Error fetching audit');
        const data = await response.json();
        renderAudit(data);
    } catch (error) {
        console.error('Error fetching audit:', error);
        throw error;
    }
}

function renderAudit(auditLogs) {
    const tbody = document.querySelector('#audit-table tbody');

    if (auditLogs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="no-data">No hay registros de auditoria</td></tr>';
        return;
    }

    tbody.innerHTML = auditLogs.map(log => {
        const resultBadge = log.result === 'SUCCESS'
            ? '<span class="badge badge-green">OK</span>'
            : '<span class="badge badge-red">ERROR</span>';

        return `
            <tr>
                <td>${log.timestamp || 'N/A'}</td>
                <td>${log.event_type || 'N/A'}</td>
                <td>${log.source_id || log.source_ip || 'N/A'}</td>
                <td>${log.action || 'N/A'}</td>
                <td>${resultBadge}</td>
            </tr>
        `;
    }).join('');
}

// ======================================================================
// FUNCIONES DE CONTROL
// ======================================================================

async function pararCP() {
    const cpId = document.getElementById('cp-select').value;
    if (!cpId) {
        showNotification('Selecciona un CP primero', 'error');
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/cp/${cpId}/parar`, { method: 'POST' });
        const data = await response.json();

        if (data.success) {
            showNotification(`CP ${cpId} parado correctamente`, 'success');
            updateAll();
        } else {
            showNotification(data.error || 'Error al parar CP', 'error');
        }
    } catch (error) {
        showNotification('Error de conexion', 'error');
    }
}

async function reanudarCP() {
    const cpId = document.getElementById('cp-select').value;
    if (!cpId) {
        showNotification('Selecciona un CP primero', 'error');
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/cp/${cpId}/reanudar`, { method: 'POST' });
        const data = await response.json();

        if (data.success) {
            showNotification(`CP ${cpId} reanudado correctamente`, 'success');
            updateAll();
        } else {
            showNotification(data.error || 'Error al reanudar CP', 'error');
        }
    } catch (error) {
        showNotification('Error de conexion', 'error');
    }
}

async function pararTodos() {
    if (!confirm('¿Seguro que quieres parar todos los CPs activos?')) return;

    try {
        const response = await fetch(`${API_BASE}/cp/parar_todos`, { method: 'POST' });
        const data = await response.json();

        if (data.success) {
            showNotification(data.message, 'success');
            updateAll();
        } else {
            showNotification(data.error || 'Error al parar todos', 'error');
        }
    } catch (error) {
        showNotification('Error de conexion', 'error');
    }
}

async function reanudarTodos() {
    if (!confirm('¿Seguro que quieres reanudar todos los CPs parados?')) return;

    try {
        const response = await fetch(`${API_BASE}/cp/reanudar_todos`, { method: 'POST' });
        const data = await response.json();

        if (data.success) {
            showNotification(data.message, 'success');
            updateAll();
        } else {
            showNotification(data.error || 'Error al reanudar todos', 'error');
        }
    } catch (error) {
        showNotification('Error de conexion', 'error');
    }
}

async function restaurarClaves() {
    if (!confirm('¡ATENCION! Esto revocara TODAS las claves de cifrado.\nLos CPs quedaran DESACTIVADOS y deberan re-autenticarse.\n\n¿Continuar?')) return;

    try {
        const response = await fetch(`${API_BASE}/restaurar_claves`, { method: 'POST' });
        const data = await response.json();

        if (data.success) {
            showNotification('Claves revocadas. Los CPs deben re-autenticarse.', 'success');
            updateAll();
        } else {
            showNotification(data.error || 'Error al restaurar claves', 'error');
        }
    } catch (error) {
        showNotification('Error de conexion', 'error');
    }
}

function showNotification(message, type) {
    // Eliminar notificacion anterior si existe
    const existing = document.querySelector('.notification');
    if (existing) existing.remove();

    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.textContent = message;
    document.body.appendChild(notification);

    // Mostrar con animacion
    setTimeout(() => notification.classList.add('show'), 10);

    // Ocultar despues de 3 segundos
    setTimeout(() => {
        notification.classList.remove('show');
        setTimeout(() => notification.remove(), 300);
    }, 3000);
}
