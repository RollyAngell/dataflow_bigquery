# Dataflow BigQuery Batch Pipeline #

Pipeline batch para procesar archivos CSV desde Google Cloud Storage (GCS) y cargarlos en BigQuery usando Apache Beam y Dataflow.

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATAFLOW PIPELINE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────┐    ┌──────────┐    ┌─────────────┐    ┌────────────────────┐  │
│  │   GCS   │───▶│  Reader  │───▶│  Validator  │───▶│   Transformer      │  │
│  │  (CSV)  │    │          │    │             │    │ (+load_timestamp)  │  │
│  └─────────┘    └──────────┘    └──────┬──────┘    └─────────┬──────────┘  │
│                                        │                      │             │
│                                        │ Invalid              │ Valid       │
│                                        ▼                      ▼             │
│                              ┌──────────────┐       ┌─────────────────┐    │
│                              │  Dead Letter │       │    BigQuery     │    │
│                              │    (GCS)     │       │  (Partitioned)  │    │
│                              └──────────────┘       └─────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Características

- **Lectura**: CSV desde GCS con soporte para encabezados y delimitador configurable
- **Validación**: Campos obligatorios, tipos de datos, formato de email
- **Transformación**: Conversión de tipos, normalización, campos de auditoría
- **Escritura**: BigQuery (tabla particionada) + Dead Letter bucket para registros inválidos
- **Métricas**: Contadores de registros procesados, válidos e inválidos
- **CI/CD**: GitHub Actions con Workload Identity Federation

## Estructura del Proyecto

```
dataflow_bigquery/
├── .github/
│   └── workflows/
│       └── dataflow.yml          # CI/CD pipeline
├── src/
│   ├── pipeline/
│   │   ├── main.py               # Entry point del pipeline
│   │   ├── options.py            # Pipeline options configurables
│   │   ├── transforms/
│   │   │   ├── readers.py        # Lectura de CSV desde GCS
│   │   │   ├── validators.py     # Validaciones de datos
│   │   │   ├── transformers.py   # Transformaciones de datos
│   │   │   └── writers.py        # Escritura a BigQuery y dead-letter
│   │   └── utils/
│   │       ├── logging_utils.py  # Métricas y logging
│   │       └── schema.py         # Esquemas de BigQuery
│   └── config/
│       └── settings.py           # Configuración centralizada
├── tests/
│   ├── test_validators.py
│   ├── test_transformers.py
│   └── test_pipeline.py
├── schemas/
│   └── bigquery_schema.json      # Esquema explícito de BigQuery
├── scripts/
│   ├── run_local.sh              # Ejecución local (Linux/Mac)
│   ├── run_local.ps1             # Ejecución local (Windows)
│   └── run_dataflow.sh           # Ejecución en GCP
├── requirements.txt
├── requirements-dev.txt
├── setup.py
└── setup.cfg
```

## Requisitos Previos

### Software
- Python 3.9+
- Google Cloud SDK (gcloud)
- Git

### GCP
- Proyecto de GCP con las APIs habilitadas:
  - Dataflow API
  - BigQuery API
  - Cloud Storage API
- Service Account con los siguientes roles:
  - `roles/dataflow.worker`
  - `roles/bigquery.dataEditor`
  - `roles/storage.objectViewer` (bucket de entrada)
  - `roles/storage.objectCreator` (bucket de dead letter)
  - `roles/storage.objectViewer` (bucket de Dataflow)

## Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/your-org/dataflow-bigquery-pipeline.git
cd dataflow-bigquery-pipeline
```

### 2. Crear entorno virtual

```bash
# Linux/Mac
python -m venv venv
source venv/bin/activate

# Windows
python -m venv venv
.\venv\Scripts\activate
```

### 3. Instalar dependencias

```bash
# Producción
pip install -r requirements.txt

# Desarrollo (incluye testing y linting)
pip install -r requirements-dev.txt

# Instalar el paquete en modo editable
pip install -e .
```

### 4. Configurar autenticación GCP

```bash
# Autenticación para desarrollo local
gcloud auth application-default login

# Establecer proyecto
gcloud config set project YOUR_PROJECT_ID
```

## Uso

### Ejecución Local (DirectRunner)

Ideal para desarrollo y testing:

```bash
# Linux/Mac
./scripts/run_local.sh --project your-project-id

# Windows PowerShell
.\scripts\run_local.ps1 -Project "your-project-id"

# Directamente con Python
python -m pipeline.main \
    --runner=DirectRunner \
    --project=your-project-id \
    --input_bucket=your-input-bucket \
    --input_file=data/input.csv \
    --output_dataset=your_dataset \
    --output_table=your_table \
    --dead_letter_bucket=your-dead-letter-bucket
```

### Ejecución en Dataflow (DataflowRunner)

Para producción:

```bash
# Linux/Mac
./scripts/run_dataflow.sh --env dev --project your-project-id

# Directamente con Python
python -m pipeline.main \
    --runner=DataflowRunner \
    --project=your-project-id \
    --region=us-central1 \
    --input_bucket=your-input-bucket \
    --input_file=data/input.csv \
    --output_dataset=your_dataset \
    --output_table=your_table \
    --dead_letter_bucket=your-dead-letter-bucket \
    --temp_location=gs://your-dataflow-bucket/temp \
    --staging_location=gs://your-dataflow-bucket/staging \
    --service_account_email=your-sa@your-project.iam.gserviceaccount.com \
    --max_num_workers=10 \
    --experiments=use_runner_v2
```

### Parámetros del Pipeline

| Parámetro | Descripción | Requerido |
|-----------|-------------|-----------|
| `--input_bucket` | Bucket GCS con el CSV de entrada | Sí |
| `--input_file` | Ruta al archivo CSV dentro del bucket | Sí |
| `--output_dataset` | Dataset de BigQuery | Sí |
| `--output_table` | Tabla de BigQuery | Sí |
| `--dead_letter_bucket` | Bucket para registros inválidos | Sí |
| `--delimiter` | Delimitador CSV (default: `,`) | No |
| `--has_header` | Si el CSV tiene encabezado (default: `True`) | No |
| `--schema_file` | Ruta al esquema JSON (default: `schemas/bigquery_schema.json`) | No |

## Esquema BigQuery

La tabla de destino se crea con el siguiente esquema (definido en `schemas/bigquery_schema.json`):

| Campo | Tipo | Modo | Descripción |
|-------|------|------|-------------|
| id | INTEGER | REQUIRED | Identificador único |
| name | STRING | REQUIRED | Nombre completo |
| email | STRING | REQUIRED | Correo electrónico |
| amount | FLOAT | NULLABLE | Monto de transacción |
| created_at | TIMESTAMP | NULLABLE | Fecha de creación original |
| is_active | BOOLEAN | NULLABLE | Estado activo |
| category | STRING | NULLABLE | Categoría |
| _load_timestamp | TIMESTAMP | REQUIRED | Timestamp de carga (auditoría) |
| _source_file | STRING | REQUIRED | Archivo fuente (auditoría) |

La tabla está particionada por `_load_timestamp` (particionamiento diario).

## Testing

```bash
# Ejecutar todos los tests
pytest tests/ -v

# Con cobertura
pytest tests/ --cov=src --cov-report=term-missing

# Solo tests de validadores
pytest tests/test_validators.py -v
```

## Linting

```bash
# Verificar formato
black --check src/ tests/
isort --check-only src/ tests/
flake8 src/ tests/

# Aplicar formato
black src/ tests/
isort src/ tests/
```

## CI/CD

El workflow de GitHub Actions (`.github/workflows/dataflow.yml`) incluye:

1. **Lint**: Verifica formato de código (flake8, black, isort)
2. **Test**: Ejecuta tests unitarios con cobertura
3. **Validate**: Valida configuración del pipeline (dry-run)
4. **Deploy Dev**: Despliega a ambiente de desarrollo
5. **Deploy Staging**: Despliega a staging (requiere aprobación)
6. **Deploy Prod**: Despliega a producción (requiere aprobación)

### Configuración de GitHub Actions

Variables requeridas en GitHub (Settings > Secrets and variables > Actions):

**Variables:**
- `GCP_PROJECT_ID`: ID del proyecto GCP
- `GCP_REGION`: Región de GCP
- `WORKLOAD_IDENTITY_PROVIDER`: Provider de Workload Identity
- `SERVICE_ACCOUNT`: Email del Service Account
- `INPUT_BUCKET_DEV`, `INPUT_BUCKET_STAGING`, `INPUT_BUCKET_PROD`
- `BQ_DATASET_DEV`, `BQ_DATASET_STAGING`, `BQ_DATASET_PROD`
- `DEAD_LETTER_BUCKET_DEV`, `DEAD_LETTER_BUCKET_STAGING`, `DEAD_LETTER_BUCKET_PROD`
- `DATAFLOW_BUCKET_DEV`, `DATAFLOW_BUCKET_STAGING`, `DATAFLOW_BUCKET_PROD`

### Configuración de Workload Identity Federation

```bash
# Crear pool de identidad
gcloud iam workload-identity-pools create "github-pool" \
    --location="global" \
    --display-name="GitHub Actions Pool"

# Crear provider
gcloud iam workload-identity-pools providers create-oidc "github-provider" \
    --location="global" \
    --workload-identity-pool="github-pool" \
    --display-name="GitHub Provider" \
    --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository" \
    --issuer-uri="https://token.actions.githubusercontent.com"

# Vincular Service Account
gcloud iam service-accounts add-iam-policy-binding "dataflow-sa@YOUR_PROJECT.iam.gserviceaccount.com" \
    --role="roles/iam.workloadIdentityUser" \
    --member="principalSet://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/github-pool/attribute.repository/YOUR_ORG/YOUR_REPO"
```

## Métricas

El pipeline expone las siguientes métricas de Apache Beam:

- `records_read`: Total de registros leídos
- `records_valid`: Registros que pasaron validación
- `records_invalid`: Registros que fallaron validación
- `records_written_bigquery`: Registros escritos a BigQuery
- `records_written_dead_letter`: Registros escritos al dead letter bucket

Estas métricas son visibles en la consola de Dataflow.

## Troubleshooting

### Error: "Permission denied"
Verificar que el Service Account tiene los roles necesarios.

### Error: "Table not found"
El pipeline crea la tabla si no existe (`CREATE_IF_NEEDED`). Verificar que el dataset existe.

### Registros en Dead Letter
Revisar los archivos JSON en el bucket de dead letter para ver los errores de validación.

### Pipeline lento
- Aumentar `max_num_workers`
- Usar un `machine_type` más grande
- Verificar el tamaño del archivo de entrada

## Licencia

MIT License - Ver [LICENSE](LICENSE) para más detalles.
