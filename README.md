# 🏎️ OpenF1 → PostgreSQL Pipeline

Proyecto para consumir **todos los endpoints de la API OpenF1** y almacenar los datos en una base de datos **PostgreSQL 18** local en Mac.

---

## 📁 Estructura del proyecto

```
openf1_project/
├── main.py           # Script principal del pipeline ETL
├── config.py         # Configuración de BD y API
├── setup_db.sh       # Script para crear la BD en PostgreSQL
├── requirements.txt  # Dependencias Python
└── README.md         # Este archivo
```

---

## 🛠️ Requisitos previos

### 1. Instalar Homebrew (si no lo tienes)

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### 2. Instalar PostgreSQL 18

```bash
brew install postgresql@18
```

### 3. Agregar PostgreSQL 18 al PATH

> ⚠️ **Este paso es crítico.** Sin él, el comando `psql` no será encontrado y el script `setup_db.sh` fallará con el error `psql: command not found`.

```bash
echo 'export PATH="/opt/homebrew/opt/postgresql@18/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

> Si usas **bash** en vez de zsh, cambia `~/.zshrc` por `~/.bash_profile`.

Verifica que funciona:
```bash
psql --version
# Debe mostrar: psql (PostgreSQL) 18.x
```

### 4. Iniciar el servicio de PostgreSQL

```bash
brew services start postgresql@18
```

Verifica que está corriendo:
```bash
brew services list
# postgresql@18 debe aparecer como "started"
```

> ⚠️ Si aparece con estado **error**, consulta la sección de Problemas frecuentes al final de este archivo.

### 5. Instalar Python (si no lo tienes)

```bash
brew install python@3.11
```

---

## ⚙️ Configuración

### Cambiar contraseña (recomendado)

Edita `config.py` y cambia el valor de `password`:

```python
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "openf1_db",
    "user":     "openf1_user",
    "password": "TU_CONTRASEÑA_SEGURA",  # ← cambia esto
}
```

Si cambias la contraseña, actualiza también la variable `DB_PASS` en `setup_db.sh`.

---

## 🚀 Instalación y primer uso

```bash
# 1. Entrar al directorio del proyecto
cd openf1_project

# 2. Crear un entorno virtual de Python
python3 -m venv .venv
source .venv/bin/activate

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Crear la base de datos y el usuario en PostgreSQL
chmod +x setup_db.sh
./setup_db.sh
```

> ⚠️ Si `setup_db.sh` falla con `psql: command not found`, significa que el PATH no está configurado. Regresa al Paso 3 de Requisitos previos, abre una terminal nueva y vuelve a intentarlo.

### Si setup_db.sh falla — crear la BD manualmente

En caso de que el script no funcione, puedes crear el usuario y la base de datos directamente desde la terminal:

```bash
# Conectarte a PostgreSQL con tu usuario de Mac (no requiere contraseña)
psql postgres
```

Una vez dentro del prompt `postgres=#`, ejecuta:

```sql
CREATE ROLE openf1_user WITH LOGIN PASSWORD 'openf1_pass';
CREATE DATABASE openf1_db OWNER openf1_user;
GRANT ALL PRIVILEGES ON DATABASE openf1_db TO openf1_user;
\q
```

Verifica que la conexión funciona:
```bash
psql -U openf1_user -d openf1_db -c "SELECT version();"
```

```bash
# 5. Ejecutar el pipeline
python main.py
```

Al correr por primera vez verás algo como:
```
✅ All tables created / verified.
  ↓ Fetching /meetings…
    ✔  3 record(s) upserted into 'meetings'
  ↓ Fetching /sessions…
    ✔  5 record(s) upserted into 'sessions'
...
🏁 Pipeline finished. Total records upserted: 1243
```

---

## 🔄 Uso del pipeline

### Obtener datos de una sesión específica

```python
# En main.py, al final del archivo:
run_pipeline(params={"session_key": 9159})
```

### Obtener datos de un piloto específico

```python
run_pipeline(params={"driver_number": 55})
```

### Obtener todos los datos disponibles (sin filtros)

```python
run_pipeline()
```

### Obtener datos de un año completo

```python
run_pipeline(params={"year": 2024})
```

> ⚠️ **Advertencia**: Obtener todos los datos sin filtros puede generar millones de registros y tardar mucho tiempo. Empieza siempre con `session_key` o `year`.

---

## 📊 Tablas en PostgreSQL

| Tabla | Descripción |
|---|---|
| `car_data` | Telemetría del coche (~3.7 Hz) — velocidad, RPM, DRS, etc. |
| `championship_drivers` | Clasificación del campeonato de pilotos |
| `championship_teams` | Clasificación del campeonato de constructores |
| `drivers` | Información de pilotos por sesión |
| `intervals` | Diferencia al líder e intervalo entre coches |
| `laps` | Tiempos de vuelta y sectores |
| `location` | Posición GPS del coche en pista |
| `meetings` | Información de cada Gran Premio |
| `overtakes` | Adelantamientos en pista |
| `pit` | Paradas en boxes |
| `position` | Posición en carrera a lo largo del tiempo |
| `race_control` | Mensajes de dirección de carrera (banderas, SC, VSC…) |
| `sessions` | Sesiones (FP1, FP2, FP3, Quali, Race) |
| `session_result` | Clasificación final de sesión |
| `starting_grid` | Posiciones de salida |
| `stints` | Información de neumáticos por stint |
| `team_radio` | URLs de clips de radio del equipo |
| `weather` | Condiciones meteorológicas en pista |

---

## 🔍 Consultas de ejemplo

```sql
-- Conectarse a la BD
psql -U openf1_user -d openf1_db

-- Ver todas las tablas
\dt

-- Contar registros por tabla
SELECT 'car_data' AS tabla, COUNT(*) FROM car_data
UNION ALL SELECT 'laps', COUNT(*) FROM laps
UNION ALL SELECT 'drivers', COUNT(*) FROM drivers
UNION ALL SELECT 'weather', COUNT(*) FROM weather
UNION ALL SELECT 'sessions', COUNT(*) FROM sessions
UNION ALL SELECT 'meetings', COUNT(*) FROM meetings;

-- Ver todos los Grandes Premios
SELECT meeting_name, country_name, year, date_start
FROM meetings
ORDER BY date_start DESC;

-- Top velocidades de la sesión 9159
SELECT driver_number, speed, rpm, n_gear, date
FROM car_data
WHERE session_key = 9159
ORDER BY speed DESC
LIMIT 10;

-- Mejores tiempos de vuelta por piloto
SELECT driver_number, lap_number, lap_duration
FROM laps
WHERE session_key = 9159
ORDER BY lap_duration ASC NULLS LAST
LIMIT 10;

-- Paradas en boxes de la sesión
SELECT driver_number, lap_number, pit_duration
FROM pit
WHERE session_key = 9159
ORDER BY pit_duration ASC;

-- Salir
\q
```

---

## 🖥️ Ver los datos con interfaz gráfica

### pgAdmin 4 (herramienta oficial de PostgreSQL)

1. Descarga en **pgadmin.org/download/pgadmin-4-macos**
2. Abre pgAdmin 4
3. Click derecho en **Servers** → **Register** → **Server...**
4. Pestaña **General** → Name: `OpenF1`
5. Pestaña **Connection**:
   - Host: `localhost`
   - Port: `5432`
   - Maintenance database: `openf1_db`
   - Username: `openf1_user`
   - Password: `openf1_pass`
   - Activa **Save password**
6. Click en **Save**

Para ver los datos: click derecho sobre cualquier tabla → **View/Edit Data** → **All Rows**.

Para correr consultas SQL: **Tools** → **Query Tool**.

### TablePlus (alternativa liviana para Mac)

Descarga en **tableplus.com** — mismos datos de conexión que pgAdmin.

---

## 📝 Logs

El pipeline genera un archivo de log `openf1_pipeline.log` en el mismo directorio con el detalle de cada ejecución.

---

## 🙋 Problemas frecuentes

**`psql: command not found` al ejecutar setup_db.sh**
→ PostgreSQL no está en el PATH. Ejecuta:
```bash
echo 'export PATH="/opt/homebrew/opt/postgresql@18/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```
Abre una terminal nueva y vuelve a intentarlo.

**`postgresql@18 error` en `brew services list`**
→ El servicio no pudo iniciar. Revisa el log con:
```bash
cat /opt/homebrew/var/log/postgresql@18.log
```
O intenta iniciar manualmente para ver el error:
```bash
/opt/homebrew/opt/postgresql@18/bin/postgres -D /opt/homebrew/var/postgresql@18
```
Las causas más comunes son: puerto 5432 ocupado por otra versión de PostgreSQL, o directorio de datos no inicializado.

**`FATAL: password authentication failed for user "openf1_user"`**
→ El usuario no fue creado correctamente porque `setup_db.sh` falló antes. Crea el usuario manualmente desde `psql postgres` (ver sección *Si setup_db.sh falla* más arriba).

**`column "rainfall" is of type boolean but expression is of type integer`**
→ La API devuelve `rainfall` como `0`/`1` en vez de `true`/`false`. En `main.py`, dentro de la función `upsert_weather`, cambia `r.get("rainfall")` por `bool(r.get("rainfall"))`.

**`connection refused` al conectar a PostgreSQL**
→ El servicio no está corriendo. Ejecuta:
```bash
brew services start postgresql@18
```

**`permission denied` al ejecutar setup_db.sh**
→ Ejecuta `chmod +x setup_db.sh` primero.

**Muchos datos / tiempo de espera**
→ Usa siempre `params={"session_key": XXXXX}` para filtrar por sesión.
