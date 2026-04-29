const express = require('express');
const cors    = require('cors');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();
app.use(express.json());

// ============================================================
// CORS — cerrado por defecto; abre solo los orígenes en FRONTEND_URLS.
// Para debug puntual en Render: CORS_OPEN=1 (nunca dejar en producción).
// ============================================================
const useOpenCors = (process.env.CORS_OPEN || '0') !== '0';
if (useOpenCors) {
  app.use(cors({ origin: true, maxAge: 3600 }));
} else {
  const DEFAULT_FRONTEND = 'https://alejoarango8a.github.io';
  const origins = (process.env.FRONTEND_URLS || DEFAULT_FRONTEND)
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  app.use(cors({
    origin(origin, cb) {
      if (!origin) return cb(null, true);
      const allowed = origins.some(entry => {
        try { return origin === new URL(entry).origin; } catch { return origin === entry; }
      });
      allowed ? cb(null, origin) : cb(new Error('Not allowed by CORS'));
    },
  }));
}

// ============================================================
// BASE DE DATOS — CockroachDB vía driver pg
// ============================================================
if (!process.env.COCKROACH_URL) {
  console.error('ERROR: falta COCKROACH_URL en las variables de entorno');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.COCKROACH_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

pool.on('error', (err) => console.error('DB pool error:', err));

// Helper: ejecuta una query y devuelve las filas
async function query(sql, params = []) {
  const client = await pool.connect();
  try {
    const res = await client.query(sql, params);
    return res.rows;
  } finally {
    client.release();
  }
}

// ============================================================
// HEALTH
// ============================================================
app.get('/health', async (req, res) => {
  try {
    await query('SELECT 1');
    res.json({ ok: true, service: 'latambanks-api', db: 'cockroachdb' });
  } catch (e) {
    res.status(503).json({ ok: false, error: String(e.message) });
  }
});

// ============================================================
// GET /api/bootstrap — períodos + instituciones + plan_cuentas + patrimonio
// ============================================================
app.get('/api/bootstrap', async (req, res) => {
  try {
    const [periodosRows, instituciones, planCuentas] = await Promise.all([
      query("SELECT periodo FROM carga_log WHERE estado = 'ok' ORDER BY periodo ASC"),
      query('SELECT codigo, razon_social FROM instituciones ORDER BY codigo ASC'),
      query('SELECT cuenta, descripcion FROM plan_cuentas ORDER BY cuenta ASC'),
    ]);

    const periodos = periodosRows.map(r => r.periodo);
    if (!periodos.length) {
      return res.status(502).json({ ok: false, error: 'No hay períodos en la base de datos' });
    }

    const lastPeriodo = periodos[periodos.length - 1];
    const patrimonioRows = await query(
      "SELECT ins_cod, monto_total FROM datos_financieros WHERE tipo = 'b1' AND cuenta = '300000000' AND periodo = $1",
      [lastPeriodo]
    ).catch(e => {
      console.warn('patrimonio ranking fetch failed (non-fatal):', e.message);
      return [];
    });

    res.json({ ok: true, periodos, instituciones, planCuentas, patrimonioRows });
  } catch (e) {
    console.error('/api/bootstrap error:', e);
    res.status(500).json({ ok: false, error: String(e.message) });
  }
});

// ============================================================
// POST /api/datos — datos financieros filtrados
// Body: { tipo|tipos[], periodos[], cuentas[], bancos[]?, select? }
// ============================================================
const ALLOWED_COLS = new Set([
  'periodo','ins_cod','cuenta','monto_total','monto_clp','monto_uf','monto_tc','monto_ext','tipo',
]);

app.post('/api/datos', async (req, res) => {
  try {
    const { tipo, tipos: tiposArr, periodos, bancos, cuentas, select: selectCols } = req.body || {};

    const tiposList = Array.isArray(tiposArr) && tiposArr.length ? tiposArr
                    : tipo ? [tipo]
                    : null;
    if (!tiposList)                                return res.status(400).json({ ok: false, error: 'Requerido: tipo o tipos[]' });
    if (!Array.isArray(periodos) || !periodos.length) return res.status(400).json({ ok: false, error: 'Requerido: periodos[]' });
    if (!Array.isArray(cuentas)  || !cuentas.length)  return res.status(400).json({ ok: false, error: 'Requerido: cuentas[]' });

    const cols = selectCols
      ? selectCols.split(',').map(c => c.trim()).filter(c => ALLOWED_COLS.has(c))
      : ['periodo','ins_cod','cuenta','monto_total','monto_clp','monto_uf','monto_tc','monto_ext'];

    const selectStr = cols.join(', ');

    // Una query por tipo — en paralelo
    const tipoPromises = tiposList.map(t => {
      const params = [t, periodos, cuentas];
      let sql = `SELECT ${selectStr} FROM datos_financieros
                 WHERE tipo = $1
                   AND periodo = ANY($2)
                   AND cuenta  = ANY($3)`;
      if (Array.isArray(bancos) && bancos.length) {
        params.push(bancos);
        sql += ` AND ins_cod = ANY($${params.length})`;
      }
      return query(sql, params);
    });

    const allRows = (await Promise.all(tipoPromises)).flat();
    res.json({ ok: true, rows: allRows });
  } catch (e) {
    console.error('/api/datos error:', e);
    res.status(500).json({ ok: false, error: String(e.message) });
  }
});

// ============================================================
// START
// ============================================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`API running on port ${PORT} — db: CockroachDB v2`));
