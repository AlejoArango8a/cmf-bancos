# LatamBanks API (backend)

Primera fase: endpoint **`GET /api/bootstrap`** que devuelve periodos e instituciones (misma lógica que antes hacía el `index.html` directo contra Supabase).

## Variables en Render

En el servicio Web → **Environment**:

| Variable | Valor |
|----------|--------|
| `SUPABASE_URL` | URL de tu proyecto Supabase (igual que en el dashboard) |
| `SUPABASE_ANON_KEY` | Clave anónima (anon public) de Supabase |
| `FRONTEND_URLS` | Opcional. Ej: `https://TU_USUARIO.github.io` (varias separadas por coma). Por defecto puedes dejar `*`. |

## Despliegue

1. Sube esta carpeta (o el repo que contenga estos archivos) a GitHub.
2. En Render: **Web Service** → conecta el repo → **Start Command**: `node server.js` → **Build Command**: `npm install`.

## Probar local

```bash
cp .env.example .env
# Edita .env con tus valores reales
npm install
npm start
```

Abre `http://localhost:3000/health` y `http://localhost:3000/api/bootstrap`.
