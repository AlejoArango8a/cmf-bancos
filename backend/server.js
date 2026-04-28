const express = require('express');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(express.json());

const origins = (process.env.FRONTEND_URLS || '*')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

app.use(
  cors({
    origin(origin, cb) {
      if (origins.includes('*') || !origin) return cb(null, true);
      if (origins.some((o) => origin === o || origin.startsWith(o))) return cb(null, true);
      return cb(null, false);
    },
  })
);

async function supabaseRest(table, queryParts) {
  const base = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_ANON_KEY;
  if (!base || !key) {
    const err = new Error('Missing SUPABASE_URL or SUPABASE_ANON_KEY');
    err.status = 503;
    throw err;
  }
  const url = `${base.replace(/\/$/, '')}/rest/v1/${table}?${queryParts.join('&')}`;
  const resp = await fetch(url, {
    headers: {
      apikey: key,
      Authorization: `Bearer ${key}`,
      'Content-Type': 'application/json',
      Range: '0-49999',
      Prefer: 'count=none',
    },
  });
  if (!resp.ok) {
    const err = new Error(`Supabase ${resp.status}: ${await resp.text()}`);
    err.status = 502;
    throw err;
  }
  return resp.json();
}

app.get('/health', (req, res) => {
  res.json({ ok: true, service: 'latambanks-api' });
});

/** Fase 1: periodos + instituciones (equivalente a las dos primeras consultas de init() en index.html) */
app.get('/api/bootstrap', async (req, res) => {
  try {
    const logs = await supabaseRest('carga_log', [
      'select=periodo',
      'estado=eq.ok',
      'order=periodo.asc',
    ]);
    const periodos = logs.map((r) => r.periodo);
    if (!periodos.length) {
      return res.status(502).json({ ok: false, error: 'No data found in database (no periods)' });
    }
    const instituciones = await supabaseRest('instituciones', [
      'select=codigo,razon_social',
      'order=codigo.asc',
    ]);
    res.json({ ok: true, periodos, instituciones });
  } catch (e) {
    const status = e.status || 500;
    res.status(status).json({ ok: false, error: String(e.message || e) });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API running on port ${PORT}`);
});
