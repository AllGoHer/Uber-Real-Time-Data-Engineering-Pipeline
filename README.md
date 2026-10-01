# 🚕 Uber-Real-Time-Data-Engineering-Pipeline
___________________________________________________________________________________________________________________________________________________________________________________________________________________

![image](https://github.com/user-attachments/assets/e6fdecca-1b98-47c8-b0bd-e52c196daca8)

___________________________________________________________________________________________________________________________________________________________________________________________________________________

![image](https://github.com/user-attachments/assets/08770ee9-8e89-421c-a3f5-7ebb8433beb0) ![image](https://github.com/user-attachments/assets/aa7a98eb-3975-4e31-8f08-1e198a282ed5) ![image](https://github.com/user-attachments/assets/e7ea03f1-1202-48e1-97d1-daec815fc01d) ![image](https://github.com/user-attachments/assets/78b9d580-40e5-4561-bad7-db01a2540dcc) ![image](https://github.com/user-attachments/assets/285f7fe1-2f6d-4db5-b310-fc2334cbd4ce) ![image](https://github.com/user-attachments/assets/8a8f8346-b315-40ed-9c8f-54a68a27fd04)

## 🛠️ Nota de Arquitectura
Este proyecto fue diseñado, orquestado y desplegado al 100% desde cero. Se implementó un pipeline de streaming stateful utilizando Spark Structured Streaming con semántica Exactly-Once, procesando eventos de Uber en tiempo real a través de la Arquitectura Medallion (Bronze → Silver → Gold) sobre Azure.


## 🎯 Resumen Ejecutivo

Este proyecto implementa una **plataforma de datos End-to-End de nivel productivo** que simula la infraestructura de datos de una plataforma de movilidad como Uber, procesando eventos de telemetría y operaciones de viaje en **tiempo real y batch**. El flujo comienza con una Web App donde se generan eventos asociados a solicitudes de viajes, ubicaciones, estados del viaje y pagos, que son ingeridos mediante **Azure Event Hubs** y procesados de forma escalable y resiliente utilizando **Azure Databricks, Apache Spark Structured Streaming y Spark Declarative Pipelines**.

La arquitectura incorpora principios modernos de **Data Engineering**, incluyendo procesamiento incremental, arquitectura **Medallion (Bronze, Silver y Gold)**, **Delta Lake**, gestión de metadatos, calidad y trazabilidad de datos, así como **Unity Catalog** para gobierno, seguridad y control de acceso. Finalmente, los datos procesados se transforman en un **modelo dimensional basado en Star Schema**, preparado para alimentar análisis operativo, dashboards y casos de Business Intelligence.

## 🚕 Descripción del Proyecto

El proyecto representa el ciclo completo de datos de un sistema de reserva de viajes, desde la **generación del evento hasta su transformación en información analítica confiable**.

Cuando un usuario solicita un viaje a través de la Web App, se generan eventos que contienen información como solicitudes, ubicaciones GPS, estados del viaje y transacciones. Estos eventos son enviados en tiempo real a **Azure Event Hubs**, que actúa como plataforma de ingesta y distribución de eventos a gran escala.

A continuación, **Azure Databricks** recibe y procesa estos streams mediante **PySpark Structured Streaming**, aplicando transformaciones, validaciones y reglas de calidad. Los datos atraviesan una arquitectura **Medallion**, pasando de una capa **Bronze** con datos crudos, a una capa **Silver** con datos limpios y transformados, y finalmente a una capa **Gold** optimizada para consumo analítico.

El procesamiento se complementa con **Spark Declarative Pipelines**, procesamiento incremental y mecanismos orientados a construir pipelines escalables, resilientes y mantenibles. **Unity Catalog** proporciona la capa de gobierno de datos, permitiendo administrar permisos, trazabilidad, descubrimiento y seguridad sobre los activos de información.

En la etapa final, los datos de negocio se modelan mediante un **Star Schema**, incorporando tablas de hechos y dimensiones para facilitar consultas analíticas y reporting. De esta manera, el proyecto transforma **eventos de movilidad generados en tiempo real en datos confiables, gobernados y preparados para análisis operativo y Business Intelligence**.

### Flujo End-to-End

**Web App → Azure Event Hubs → Azure Databricks → Spark Structured Streaming / Declarative Pipelines → Delta Lake / Medallion Architecture → Unity Catalog → Star Schema → Analytics / BI**

El resultado es una arquitectura que demuestra cómo diseñar y construir un **pipeline moderno de Data Engineering orientado a tiempo real**, integrando ingesta de eventos, procesamiento distribuido, almacenamiento Delta, transformación incremental, gobierno de datos y modelado analítico dentro del ecosistema **Microsoft Azure + Databricks**.

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🏗️ Arquitectura de Alto Nivel

![image](https://github.com/user-attachments/assets/eb783301-c7e0-493a-9f55-1677494909f3)



## 🛠️ Stack Tecnológico Detallado

La arquitectura integra servicios nativos de **Microsoft Azure** y tecnologías de **Apache Spark/Databricks** para construir un pipeline de datos **End-to-End, escalable, resiliente y orientado a procesamiento en tiempo real**, desde la generación de eventos hasta el consumo analítico.

| **Capa / Categoría**                  | **Tecnología**                                           | **Rol en el Proyecto**                                  | **Valor / Justificación Arquitectónica**                                                                                                                                    |
| ------------------------------------- | -------------------------------------------------------- | ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Simulación de eventos**             | **FastAPI + Jinja2**                                     | Simulación de la Web App de reservas                    | Genera eventos de negocio representativos de una plataforma de movilidad: solicitudes de viaje, ubicaciones, estados y operaciones.                                         |
| **Ingesta Streaming**                 | **Azure Event Hubs**                                     | Captura y distribución de eventos en tiempo real        | Servicio de ingesta masiva de eventos compatible con el ecosistema Kafka, diseñado para manejar grandes volúmenes de datos y flujos de alta concurrencia.                   |
| **Orquestación / Batch**              | **Azure Data Factory (ADF)**                             | Orquestación de cargas, notebooks y procesos batch      | Permite construir pipelines parametrizados para coordinar procesos, cargas históricas, configuraciones y ejecuciones programadas.                                           |
| **Procesamiento Streaming**           | **Azure Databricks + Apache Spark Structured Streaming** | Procesamiento distribuido de eventos en tiempo real     | Motor de procesamiento escalable para transformar streams, aplicar ventanas temporales, realizar joins y ejecutar lógica de negocio sobre datos en movimiento.              |
| **Pipelines Declarativos**            | **Spark Declarative Pipelines (SDP/DLT)**                | Construcción y gestión de pipelines de datos            | Simplifica la implementación de transformaciones y dependencias entre datasets, facilitando pipelines mantenibles, escalables y orientados a calidad de datos.              |
| **Procesamiento / Lógica de Negocio** | **PySpark + SQL**                                        | Transformación, enriquecimiento y validación de datos   | Implementa reglas de negocio, transformaciones distribuidas, cálculos como distancia mediante **Haversine**, clasificación de precios y preparación de datasets analíticos. |
| **Data Lake**                         | **Azure Data Lake Storage Gen2**                         | Almacenamiento centralizado de datos                    | Proporciona almacenamiento escalable para datos históricos y de streaming dentro de una arquitectura Lakehouse.                                                             |
| **Formato Transaccional**             | **Delta Lake**                                           | Persistencia de las tablas de datos                     | Aporta transacciones **ACID**, control de versiones mediante **Time Travel**, operaciones **MERGE/UPSERT**, Schema Enforcement y procesamiento incremental.                 |
| **Arquitectura de Datos**             | **Medallion Architecture**                               | Organización de los datos en **Bronze → Silver → Gold** | Separa datos crudos, datos procesados y datasets preparados para consumo analítico, facilitando calidad, trazabilidad y reutilización.                                      |
| **Gobierno de Datos**                 | **Unity Catalog**                                        | Seguridad, gobierno y trazabilidad                      | Centraliza el control de acceso, permisos, descubrimiento y **data lineage** sobre los activos de datos a través de las diferentes capas.                                   |
| **Modelado Dimensional**              | **Star Schema**                                          | Modelo analítico compuesto por **Fact + 6 Dimensions**  | Convierte los datos procesados en un modelo optimizado para consultas analíticas, reporting y Business Intelligence.                                                        |
| **Control de Versiones**              | **Git + GitHub**                                         | Gestión del código fuente y colaboración                | Permite versionar notebooks, scripts, pipelines y componentes del proyecto, favoreciendo reproducibilidad, trazabilidad y buenas prácticas de desarrollo.                   |

### Flujo tecnológico

**FastAPI + Jinja2**

↓

**Azure Event Hubs**

↓

**Azure Databricks + Spark Structured Streaming**

↓

**PySpark + SQL**

↓

**Delta Lake / Azure Data Lake Storage Gen2**

↓

**Medallion Architecture — Bronze → Silver → Gold**

↓

**Unity Catalog — Governance & Lineage**

↓

**Star Schema — Fact + Dimensions**

↓

**Analytics / Business Intelligence**


![image](https://github.com/user-attachments/assets/23736d9b-7067-4eac-8c4d-781a5851991f)


**Stack principal:**
`Azure Event Hubs` · `Azure Databricks` · `Apache Spark` · `PySpark` · `Spark Structured Streaming` · `Spark Declarative Pipelines` · `Delta Lake` · `ADLS Gen2` · `Azure Data Factory` · `Unity Catalog` · `SQL` · `FastAPI` · `Git/GitHub`


### Capacidades de Data Engineering demostradas

* **Real-Time Data Streaming**
* **Distributed Data Processing**
* **Batch & Streaming Integration**
* **Lakehouse Architecture**
* **Medallion Architecture**
* **Delta Lake & ACID Transactions**
* **Incremental Processing**
* **Stream-Stream Processing & Windowing**
* **Data Quality & Validation**
* **Data Governance & Lineage**
* **Dimensional Data Modeling**
* **Metadata / Parameter-Driven Pipelines**
* **Cloud Data Engineering en Microsoft Azure**
* **Version Control & Reproducible Data Pipelines**

## 📂 Arquitectura Medallion (El Corazón del Pipeline)

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
### 🥉 Bronze Layer (Ingesta Cruda & Append-Only)
__________________________________________________________________________________________________________________________________

**Objetivo:** Ingesta de datos de forma más rápida posible sin transformaciones pesadas.

**Tecnología:** readStream desde Event Hubs.

**Acción:** Convierte el payload binario de Event Hubs a JSON y aplica writeStream en formato Delta con Append Mode.

**Regla:** No se eliminan duplicados aquí. Es la fuente de verdad cruda.

_____________________________________________________________________________________________________________________________________

### 🥈 Silver Layer (OBT - One Big Table, Limpieza, Deduplicación & Enrichment)
_____________________________________________________________________________________________________________________________________

**Objetivo:** Datos limpios, tipados y listos para análisis. Resolución de calidad de datos.

**Tecnología:** readStream desde Bronze con Change Data Feed (CDF) habilitado.

**Acción:**

- Manejo de eventos tardíos (Watermarks).

- Deduplicación usando dropDuplicates dentro de ventanas de tiempo.

- Enriquecimiento: Join del stream de viajes con tabla estática de perfiles de conductores.
  
- Cálculos espaciales (Lat/Lon a Zonas de Uber).

______________________________________________________________________________________________________________________________________  
### 🥇 Gold Layer (Agregaciones de Negocio para BI)
______________________________________________________________________________________________________________________________________

**Objetivo:** Tablas de hechos y dimensiones altamente optimizadas para consumo de dashboards.

**Tecnología:** readStream desde Silver usando Complete Mode o Update Mode para agregaciones.

**Acción:**

- Ventanas Tumble: Viajes completados por zona cada 5 minutos.

- Ventanas Slide: Cálculo de Surge Pricing (precio dinámico) basado en demanda de los últimos 15 minutos.

- Escritura final a Delta Tables gobernadas por Unity Catalog.

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📂 Modelo de Datos (Star Schema)
____________________________________________________________________________________________________________________________________________________________________________________________________________________________
La capa Gold contiene 1 tabla de hechos + 6 tablas de dimensiones :

| Tipo | Tabla | Descripción |
|------|-------|-------------|
| Fact | fact_rides | Datos de hechos de viajes |
| Dim | dim_booking | Información de reservas |
| Dim | dim_driver | Dimensión del conductor |
| Dim	| dim_passenger | Dimensión del pasajero |
| Dim | dim_payment | Método de pago |
| Dim | dim_vehicle | Información del vehículo |
| Dim	| dim_location | Ubicación geográfica |

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🏗️ Estructura del Proyecto
____________________________________________________________________________________________________________________________________________________________________________________________________________________________

A continuación, la estructura completa del repositorio, mapeada a cada etapa del pipeline. Este desglose está pensado para el entendimiento de qué hace cada archivo, por qué existe y cómo se conecta con el flujo de datos de extremo a extremo.

### 📁 Raíz del Proyecto

![image](https://github.com/user-attachments/assets/9b5f41d5-0c37-469c-92fb-f5b3aaedc7dc)


____________________________________________________________________________________________________________________________________________________________________________________________________________________________
### 🔍 Detalle de Cada Componente

#### 1. Data/ — Datos Históricos y Mapeos

**Propósito:** Almacenar los archivos JSON que actúan como datos históricos para el pipeline batch (ingesta desde GitHub vía ADF) y como tablas de referencia para enriquecimiento en la capa Silver.

![image](https://github.com/user-attachments/assets/e4c59064-6575-4f15-bc6b-f3c057fa2870)

**Impacto en el pipeline:** Estos archivos alimentan la ingesta batch mediante Azure Data Factory, que los copia dinámicamente desde GitHub a ADLS Gen2 (capa Bronze). Posteriormente, en la capa Silver, se utilizan como tablas de dimensión para resolver claves foráneas y enriquecer los datos de viajes en tiempo real.

#### 2. api.py — Punto de Entrada de la Web App

**Propósito:** Simula el sistema de reservas de Uber mediante una aplicación FastAPI con dos endpoints.

**Endpoints principales**

**1. GET /** ---> Esto renderiza la página de inicio (home.html)

codigo:

        uvicorn api:app --reload 

![image](https://github.com/user-attachments/assets/425b4cf1-0a21-4342-9e39-433eb511af57)


![image](https://github.com/user-attachments/assets/a500ca5d-4168-4c5b-8e5d-7507c2d0df75)

Aquí haremos una reserva de viaje haciendo click en Book a Ride


**2. GET  /book**  ---> Genera un viaje aleatorio y lo envía a Event Hubs

![image](https://github.com/user-attachments/assets/84c50db6-de68-4f45-ae81-4c9c95d2fb17)

**Impacto en el pipeline:**

- **/book** es el disparador de eventos en tiempo real. Cada vez que un usuario hace clic en "Book a Ride", se genera un objeto de viaje (ride confirmation) y se envía a Azure Event Hubs a través de connection.py.

Este es el punto de entrada del streaming en vivo que alimenta la capa Bronze en Databricks.
____________________________________________________________________________________________________________________________

#### 3. connection.py — Productor de Event Hubs

**Propósito:** Gestionar la conexión y el envío de datos hacia Azure Event Hubs (Kafka gestionado).


**Funciones principales:**

![image](https://github.com/user-attachments/assets/b2d4e88f-02fd-4016-93b8-24f3e9a4f411)

**Impacto en el pipeline:**

Es el puente entre la Web App y el sistema de mensajería.

Utiliza azure-eventhub SDK para publicar eventos en el topic configurado en .env.

Los datos enviados aquí son inmutables y se convierten en la fuente de verdad cruda para la capa Bronze en Databricks.

#### 4. <mark>data.py</mark> — Generador de Datos Sintéticos

**Propósito:** Generar objetos de viaje realistas que simulan las confirmaciones de reserva de Uber.

Estructura del objeto generado (generate_uber_ride_confirmation()):

![image](https://github.com/user-attachments/assets/fe3b27d6-e591-436b-8ebe-efa5ec4b55dc)

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()
