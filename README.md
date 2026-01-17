<div align="center">

# 🦈 A-tunes Invoicing ETL Pipeline
### Arquitectura Medallion en Azure Databricks


*Pipeline automatizado de datos para análisis de ventas y facturación con arquitectura de tres capas y despliegue continuo*

</div>

---

## 🎯 Descripción

Pipeline ETL enterprise-grade que transforma datos crudos de ventas y garantias de tiendas virtuales de musica de A-tunes de diferentes años y paises, implementando la **Arquitectura Medallion** (Bronze-Silver-Gold) en Azure Databricks con **CI/CD completo** y **Delta Lake** para garantizar consistencia ACID.

### ✨ Características Principales

- 🔄 **ETL Automatizado** - Pipeline completo con despliegue automático via GitHub Actions
- 🏗️ **Arquitectura Medallion** - Separación clara de capas Bronze → Silver → Gold
- 📊 **Modelo Dimensional** - Star Schema optimizado para análisis de negocio
- 🚀 **CI/CD Integrado** - Deploy automático en cada push a master
- 📈 **Power Bi Dashboards** - Visualización
- ⚡ **Delta Lake** - ACID transactions y time travel capabilities
- 🔔 **Monitoreo** - Notificaciones automáticas y logs detallados

---

## 🏛️ Arquitectura

### Flujo de Datos

```
📄 CSV (Raw Data)
    ↓
🥉 Bronze Layer (Ingesta sin transformación)
    ↓
🥈 Silver Layer (Limpieza + Modelo Dimensional)
    ↓
🥇 Gold Layer (Agregaciones de Negocio)
    ↓
📊 Databricks Dashboards (Visualización)
```




### 📦 Capas del Pipeline

<table>
<tr>
<td width="33%" valign="top">

#### 🥉 Bronze Layer
**Propósito**: Zona de aterrizaje

**Tablas**: 
- `Invoices` 
- `Invoice Lines` 
- `Tracks` 

**Características**:
- ✅ Datos tal como vienen de origen
- ✅ Timestamp de ingesta
- ✅ Preservación histórica
- ✅ Sin validaciones

</td>
<td width="33%" valign="top">

#### 🥈 Silver Layer
**Propósito**: Modelo dimensional

**Tablas**:
- `Track Sales`


**Características**:
- ✅ Star Schema
- ✅ Datos normalizados
- ✅ Validaciones completas

</td>
<td width="33%" valign="top">

#### 🥇 Gold Layer
**Propósito**: Analytics-ready

**Tablas**:
- kpi_category_sales        : Monto total en ventas agrupado por categoría y año


**Características**:
- ✅ Pre-agregados
- ✅ Optimizado para BI
- ✅ Performance máximo
- ✅ Actualizaciones automáticas

</td>
</tr>
</table>

---

## 📁 Estructura del Proyecto

```
Etl-A-Tunes/
│
├── 📂 .github/
│   └── 📂 workflows/
│       └── 📄 deploy_dev_to_prod.yml    # Pipeline CI/CD deploy a certification workspace databricks
├── 📂 ETL/
│   ├── 🐍 Ingest_Invoice_data.py       # Bronze layer
│   ├── 🐍 Ingest_Invoiceline_data.py   # Bronze Layer
│   ├── 🐍 Ingest_Tracks_data.py        # Bronze Layer
│   ├── 🐍 Transform.py                 # Silver Layer
│   ├── 🐍 Load.py        # Golden Layer
│   ├── 🐍 Preparacion de Ambiente.py    # Create Schema, Tables, External location
├── 📂 Security/
|   ├── 🐍 Grants.py               # Sql Grant
├── 📂 Revoke/
|   ├── 🐍 revoke.py               # Revoke permissions
├── 📂 Dashboards/                              
|   ├── A-tunes.pbix
|   ├── ATunes Dashboard.pdf        # PrintScreen collection
|   ├──   Dashboard.png             # PrintScreen 
├── 📂 Chinook Dataset
|   ├── ER Diagram Chinook.pdf
|   ├── Invoice.csv
|   ├── InvoiceLine.csv
|   ├── Tracks.csv
└── 📄 README.md
```

---

## 🛠️ Tecnologías

<div align="center">

| Tecnología | Propósito |
|:----------:|:----------|
| ![Databricks](https://img.shields.io/badge/Azure_Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white) | Motor de procesamiento distribuido Spark |
| ![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat-square&logo=delta&logoColor=white) | Storage layer con ACID transactions |
| ![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apache-spark&logoColor=white) | Framework de transformación de datos |
| ![ADLS](https://img.shields.io/badge/ADLS_Gen2-0078D4?style=flat-square&logo=microsoft-azure&logoColor=white) | Data Lake para almacenamiento persistente |
| ![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=flat-square&logo=github-actions&logoColor=white) | Automatización CI/CD |
| ![Databricks Dashboards](https://img.shields.io/badge/Power_BI-Power_BI_Data_Analyst_Associate-FEB800) |  Visualización |

</div>

---

## ⚙️ Requisitos Previos

- ☁️ Cuenta de Azure con acceso a Databricks
- 💻 Workspace de Databricks configurado
- 🖥️ Cluster activo (nombre: `Cluster_SD`)
- 🐙 Cuenta de GitHub con permisos de administrador
- 📦 Azure Data Lake Storage Gen2 configurado
- 📊 Power BI Desktop (visualización)

---

## 🚀 Instalación y Configuración

### 1️⃣ Clonar el Repositorio

```bash
https://github.com/eugsalazar/Proyecto-A-tunes-
cd project-databricks
```

### 2️⃣ Configurar Databricks Token

1. Ir a Databricks Workspace
2. **User Settings** → **Developer** → **Access Tokens**
3. Click en **Generate New Token**
4. Configurar:
   - **Comment**: `GitHub CI/CD`
   - **Lifetime**: `90 days`
5. ⚠️ Copiar y guardar el token

### 3️⃣ Configurar GitHub Secrets

En tu repositorio: **Settings** → **Secrets and variables** → **Actions**

| Secret Name | Valor Ejemplo |
|------------|---------------|
| `DATABRICKS_HOST` | `https://adb-xxxxx.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | `dapi_xxxxxxxxxxxxxxxx` |

### 4️⃣ Verificar Storage Configuration

```python
storage_path = "abfss://chinook@adlssmartdata0878.dfs.core.windows.net"
```

<div align="center">

✅ **¡Configuración completa!**

</div>

---

## 💻 Uso

### 🔄 Despliegue Automático (Recomendado)

```bash
git add .
git commit -m "✨ feat: mejoras en pipeline"
git push origin master
```

**GitHub Actions ejecutará**:
- 📤 Deploy de notebooks a `/py/scripts/main/`
- 🔧 Creación del workflow `WF_ADB`
- ▶️ Ejecución completa:  Bronze → Silver → Gold
- 📧 Notificaciones de resultados

### 🖱️ Despliegue Manual desde GitHub

1. Ir al tab **Actions** en GitHub
2. Seleccionar **Deploy ETL A-Tunes**
3. Click en **Run workflow**
4. Seleccionar rama `main`
5. Click en **Run workflow**

### 🔧 Ejecución Local en Databricks

Navegar a `/py/scripts/main/` y ejecutar en orden:

```
- Preparacion_Ambiente.py           → Crear esquema
- Ingest_Invoice_data.py            → Bronze Layer
- ingest_Invoiceline_data.py        → Bronze Layer
- Ingest_Tracks_data.py             → Bronze Layer
- Transform.py                      → Silver Layer
- Load.py                           → Gold Layer
```

---


## 🔄 CI/CD

### Pipeline de GitHub Actions

```yaml
Workflow: Deploy ETL Apple Sales And Warranty
├── Deploy notebooks → /py/scripts/main/
├── Eliminar workflow antiguo (si existe)
├── Buscar cluster configurado
├── Crear nuevo workflow con 6 tareas
├── Ejecutar pipeline automáticamente
└── Monitorear y notificar resultados
```

### 🔄  Workflow Databricks
![Texto descriptivo](CICD_ETL_A-Tunes.png)
```


⏰ Schedule: Diario 8:00 AM (Lima)
⏱️ Timeout total: 4 horas
 🔒 Max concurrent runs: 1
⏰ Notificaciones: 
      success: eugsalazar@gmail.com
      failed:  eugsalazar@gmail.com
```

---

## 📈 Dashboards
https://github.com/eugsalazar/Proyecto-A-tunes-/blob/main/Dashboard/A-tunes.pbix

## 🔍 Monitoreo

### En Databricks

**Workflows**:
- Ir a **Workflows** en el menú lateral
- Buscar `A-Tunes`
- Ver historial de ejecuciones

**Logs por Tarea**:
- Click en una ejecución específica
- Click en cada tarea para ver logs detallados
- Revisar stdout/stderr en caso de errores

### En GitHub Actions

- Tab **Actions** del repositorio
- Ver historial de workflows
- Click en ejecución específica para detalles
- Revisar logs de cada step

---

## 👤 Autor

<div align="center">

### Eugenio Salazar Gómez 

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/eugeniosalazarg/)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/eugsalazar)
[![Email](https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:eugsalazar@gmail.com)

**Data Engineering** | **Azure Databricks** | **Delta Lake** | **CI/CD**

</div>

---

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para más detalles.

---

<div align="center">

**Proyecto**: Data Engineering - Arquitectura Medallion  
**Tecnología**: Azure Databricks + Delta Lake + CI/CD  
**Última actualización**: 2026


</div>
