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

## 🏗️ Arquitectura de Flujo

imagen:

                                        ┌─────────────────────────────────────────────────────────────────────────────┐
                                        │                         FLUJO DE DATOS END-TO-END                           │
                                        └─────────────────────────────────────────────────────────────────────────────┘

                                [Web App: api.py]
                                      │
                                      │ POST /book → generate_uber_ride_confirmation() [data.py]
                                      ▼
                       [connection.py: send_to_event_hub()]
                                      │
                                      │ JSON serializado → EventData
                                      ▼
                            ┌───────────────────┐
                            │  Azure Event Hubs │  ← Topic: "ubertopic"
                            │ (Kafka gestionado)│
                            └─────────┬─────────┘
                                      │
                                      │ readStream (Spark Structured Streaming)
                                      ▼
    ┌─────────────────────────────────────────────────────────────────────────────┐         ┌─────────────────────────────────────────────────────────────────────────────┐
    |           DATABRICKS — BRONZE LAYER                                         |         │                         FLUJO BATCH (ADF)                                   │
    │  • Ingesta cruda, Append-Only, sin transformaciones                         │         │  GitHub (Data/*.json) → ADF Lookup (files_array.json) → ForEach → Copy      │
    │  • Delta Table: eventos JSON tal como llegan                                │         │  → ADLS Gen2 (Bronze) → Databricks (Silver)                                 │
    └─────────────────────────────────────────────────────────────────────────────┘         └───────────────────────────────┬─────────────────────────────────────────────┘
                                    │                                                                                       │
                                    │                                                                                       │
                                    │ readStream → transformaciones                                                         │
                                    ▼                                                                                       │
    ┌─────────────────────────────────────────────────────────────────────────────┐                                         │
    │                         DATABRICKS — SILVER LAYER                           │                                         │
    │  • Deduplicación por ride_id                                                │                                         │
    │  • Enriquecimiento con tablas de mapeo (Data/*.json)                        │ <---------------------------------------│
    │  • Limpieza y validación de calidad                                         │
    │  • Delta Table: datos conformados                                           │
    └───────────────────────────────┬─────────────────────────────────────────────┘
                                    │
                                    │ read → agregaciones, joins
                                    ▼
    ┌─────────────────────────────────────────────────────────────────────────────┐
    │                         DATABRICKS — GOLD LAYER                             │
    │  • Star Schema: fact_rides + 6 dimensiones                                  │
    │  • Optimizado para consultas analíticas (Power BI, dashboards)              │
    └─────────────────────────────────────────────────────────────────────────────┘



____________________________________________________________________________________________________________________________________________________________________________________________________________________________
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


![image](https://github.com/user-attachments/assets/23736d9b-7067-4eac-8c4d-781a5851991f)


**Stack principal:**
`Azure Event Hubs` · `Azure Databricks` · `Apache Spark` · `PySpark` · `Spark Structured Streaming` · `Spark Declarative Pipelines` · `Delta Lake` · `ADLS Gen2` · `Azure Data Factory` · `Unity Catalog` · `SQL` · `FastAPI` · `Git/GitHub`

____________________________________________________________________________________________________________________________________________________________________________________________________________________________

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

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
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
________________________________________________________________________________________________________________________________________________________________________________________________________________________
### 📁 Raíz del Proyecto

![image](https://github.com/user-attachments/assets/4d2bb76b-5378-4664-a646-8e48ba568050)


____________________________________________________________________________________________________________________________________________________________________________________________________________________________
### 🔍 Detalle de Cada Componente
_________________________________________________________________________________________________________________________________________________________________________________
#### 1. <mark>Data/</mark> — Datos Históricos y Mapeos

**Propósito:** Almacenar los archivos JSON que actúan como datos históricos para el pipeline batch (ingesta desde GitHub vía ADF) y como tablas de referencia para enriquecimiento en la capa Silver.

![image](https://github.com/user-attachments/assets/e4c59064-6575-4f15-bc6b-f3c057fa2870)

**Impacto en el pipeline:** Estos archivos alimentan la ingesta batch mediante Azure Data Factory, que los copia dinámicamente desde GitHub a ADLS Gen2 (capa Bronze). Posteriormente, en la capa Silver, se utilizan como tablas de dimensión para resolver claves foráneas y enriquecer los datos de viajes en tiempo real.

____________________________________________________________________________________________________________________________________________________________________________
#### 2. <mark>api.py</mark> — Punto de Entrada de la Web App

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
__________________________________________________________________________________________________________________________________________________________________________

#### 3. <mark>connection.py</mark> — Productor de Event Hubs

**Propósito:** Gestionar la conexión y el envío de datos hacia Azure Event Hubs (Kafka gestionado).


**Funciones principales:**

![image](https://github.com/user-attachments/assets/b2d4e88f-02fd-4016-93b8-24f3e9a4f411)

**Impacto en el pipeline:**

- Es el puente entre la Web App y el sistema de mensajería.

- Utiliza azure-eventhub SDK para publicar eventos en el topic configurado en .env.

- Los datos enviados aquí son inmutables y se convierten en la fuente de verdad cruda para la capa Bronze en Databricks.

_______________________________________________________________________________________________________________________________________________________________________
#### 4. <mark>data.py</mark> — Generador de Datos Sintéticos

**Propósito:** Generar objetos de viaje realistas que simulan las confirmaciones de reserva de Uber.

Estructura del objeto generado (<mark>generate_uber_ride_confirmation()</mark>):

![image](https://github.com/user-attachments/assets/fe3b27d6-e591-436b-8ebe-efa5ec4b55dc)


**Impacto en el pipeline:**

- Los IDs de claves foráneas (ej. vehicle_type_id, payment_method_id) coinciden con los mapeos en Data/, lo que permite joins eficientes en la capa Silver.

- El esquema generado está diseñado para modelado dimensional desde el origen, facilitando la construcción del Star Schema en la capa Gold.

________________________________________________________________________________________________________________________________________________________________________________
#### 5. <mark>files_array.json</mark> — Configuración de Ingesta Batch

**Propósito:** Definir la lista de archivos que ADF debe descargar desde GitHub en el pipeline batch.

json:

      [
        {"file": "map_cities"},
        {"file": "map_cancellation_reasons"},
        {"file": "bulk_rides"},
        {"file": "map_payment_methods"},
        {"file": "map_ride_statuses"},
        {"file": "map_vehicle_makes"},
        {"file": "map_vehicle_types"}
      ]


**Impacto en el pipeline:**

- Este archivo se sube a ADLS Gen2 y se lee mediante una actividad Lookup en ADF.

- El resultado se pasa a un ForEach que itera sobre cada archivo, ejecutando una actividad Copy que descarga dinámicamente @{item().file}.json desde GitHub .

- Sin este archivo, la ingesta batch de datos históricos no se ejecuta.

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
#### 6. <mark>pyproject.toml</mark> / <mark>requirements.txt</mark> / <mark>uv.lock</mark> — Gestión de Dependencias

**Propósito:** Definir y bloquear las dependencias del proyecto para garantizar reproducibilidad en cualquier entorno.

**Dependencias clave:**

| Paquete | Rol en el proyecto |
|---------|--------------------|
| azure-eventhub | Cliente para enviar/recibir eventos desde Event Hubs |
| faker | Generación de datos sintéticos realistas (nombres, direcciones, emails) |
| fastapi / uvicorn | Framework web y servidor ASGI para la Web App |
| jinja2 | Motor de plantillas para renderizar HTML |
| python-dotenv | Carga de variables de entorno desde .env |

**Impacto en el pipeline:**

- <mark>uv.lock</mark> garantiza que todos los entornos (desarrollo, staging, producción) usen exactamente las mismas versiones, evitando el clásico "funciona en mi máquina" .

- <mark>pyproject.toml</mark> usa el build backend <mark>uv_build</mark>, lo que indica modernidad en el toolchain y conocimiento de prácticas actuales de Python.

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🚀 Inicio Rápido

**1. Preparación del entorno**

bash:

        # Instalar uv (recomendado)
        pip install uv
        
        # Clonar el proyecto
        git clone https://github.com/AllGoHer/Uber-Real-Time-Data-Engineering-Pipeline.git
        cd Uber_Data_Engineer_Project
        
        # Instalar dependencias
        uv sync
        # o usando pip
        pip install -r requirements.txt

________________________________________________________________________________________________________________________________________
**2. Configurar Event Hub**

Crea un archivo .env en la raíz del proyecto:

env:

      CONNECTION_STRING="Endpoint=sb://<tu-namespace>.servicebus.windows.net/;SharedAccessKeyName=<nombre-policy>;SharedAccessKey=<tu-clave>"
      EVENT_HUBNAME="ubertopic"
      EVENT_HUB_NAMESPACE="<tu-namespace>"

____________________________________________________________________________________________________________________________________________
**3. Iniciar la Web App**

bash:

      python api.py


Accede a http://localhost:8000 y haz clic en "Book a Ride" para disparar el flujo de datos hacia Event Hub.

_______________________________________________________________________________________________________________________________________________
**4. Verificar el envío de datos**


bash:

      python connection.py

      
Si en la terminal aparece Successfully sent to Event Hub, el envío fue exitoso.


**Puntos Clave de Configuración de ADF**

- Linked Service (GitHub): URL Base = https://raw.githubusercontent.com/

- Dataset: Usar el parámetro p_file para construir la URL dinámica: .../Data/@{dataset().p_file}

- Iteración ForEach: Expresión @activity('ds_files_array').output.value para recorrer la lista de archivos

- Copy Activity origen: Valor del parámetro @{item().file}.json (atención al sufijo .json)

__________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🧠 DESARROLLO DEL PROYECTO Y PRUEBA VISUAL (VISUAL PROOF) 📸 
__________________________________________________________________________________________________________________________________________________________________________________________________________________
En este capitulo, veremos el desarrollo del proyecto Uber Real-Time paso a paso con evidencias visuales de principio a fin.


1.	Entramos a Portal.azure.com y, luego en el buscador de azure escribimos SOURCE MANAGER y hacemos click en grupo de recursos.

2.	Creamos un recurso nuevo, le asignamos un nombre y damos click en revisar y crear.

![image](https://github.com/user-attachments/assets/a7ed7744-4ea6-4401-a36d-da544bcde9e6)

<br><br>

![image](https://github.com/user-attachments/assets/017cded8-6f74-4030-b971-5521eab18d98)

<br><br>

![image](https://github.com/user-attachments/assets/bdea2436-aa15-4d17-979c-31acf5db9db9)

<br><br>

![image](https://github.com/user-attachments/assets/f128dc96-9f83-4314-9225-a37f7f5f7b6f)
<br><br>

![image](https://github.com/user-attachments/assets/ac48ba47-b277-44d8-a358-97ab4eb5c36b)
<br><br>

3.	Ahora, vamos a crear un Even Hubs y, procederemos a escribir even hubs en el buscador y seleccionamos.
   
![image](https://github.com/user-attachments/assets/59b21182-b795-4b07-ad4d-eb4df19bb4ef)

Luego, hacemos click en crear y le asignamos un nombre.

![image](https://github.com/user-attachments/assets/f53d521c-e609-4440-80ba-4943dc638061)
<br><br>

![image](https://github.com/user-attachments/assets/c5e86fcc-a7a4-4d7c-a0ec-ae3241ac1c7b)
<br><br>

![image](https://github.com/user-attachments/assets/9c8433a7-d682-49da-acbb-948460fa0676)

Ahora, volvemos a grupo de recursos para verificar dentro de ella este EventosUber.

![image](https://github.com/user-attachments/assets/1324b4f8-ea79-414b-a0bd-a8b7e491750f)
<br><br>

![image](https://github.com/user-attachments/assets/fb7425b8-431d-4ce3-b2aa-865ef67d04a8)
<br><br>

![image](https://github.com/user-attachments/assets/d05991b8-fcaf-4b3d-9c10-e35b8881b9f0)
<br><br>

![image](https://github.com/user-attachments/assets/b4f2fed3-1021-436a-9ad7-d7f4c74ad208)

Creamos un topic.

![image](https://github.com/user-attachments/assets/9bd6e4b6-f046-4990-8cb8-60ed6db09408)
<br><br>

![image](https://github.com/user-attachments/assets/ca340f91-8f8c-4f22-b6af-99fddf93fcc5)
<br><br>

![image](https://github.com/user-attachments/assets/6f1c2054-2730-40c2-8a49-96c3545456fb)
<br><br>

![image](https://github.com/user-attachments/assets/cb734cc3-b578-4e29-b173-20d57964c6d3)
<br><br>

![image](https://github.com/user-attachments/assets/bb24a113-a203-4bb1-a81c-3d02a157cd7e)
<br><br>

![image](https://github.com/user-attachments/assets/9cb312bf-3465-4d09-8932-4be29b04b60b)

Ahora, veremos las directivas del acceso compartido para los envíos. Así es que, nos dirigimos a configuración>directivas de acceso compartido y damos click en +agregar.

![image](https://github.com/user-attachments/assets/17cad7c0-9325-4307-8c11-bf6494d42d2b)
<br><br>

![image](https://github.com/user-attachments/assets/44aa2807-bf18-4f43-9980-dfe4bf487925)
<br><br>

![image](https://github.com/user-attachments/assets/3b7b641d-5977-4fcd-88a2-2ded1bb1dc21)

Ahora crearemos una política de escucha, lo haremos de la misma manera del paso anterior y solo pondremos el nombre ListenPolicy y seleccionamos escuchar.

![image](https://github.com/user-attachments/assets/a90e19d3-5f7c-4b36-a1be-9f8530b9dfd3)

Ahora, veremos los prerrequisitos para trabajar con el Centro de Eventos (Even Hub). Para ello, vamos al siguiente vinculo: https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-python-get-started-send?tabs=passwordless%2Croles-azure-portal


![image](https://github.com/user-attachments/assets/7e4050ac-a422-4bdd-b1d7-1a159f13b51c)
<br><br>

![image](https://github.com/user-attachments/assets/e0f70c25-75d1-4612-92f7-b3216e86d78b)
<br><br>

![image](https://github.com/user-attachments/assets/e0c7f477-f1b3-438e-bf53-ca0856775e13)
<br><br>

![image](https://github.com/user-attachments/assets/ba39ca11-e582-4e38-a135-0a2d312b35d0)

Código:
        import asyncio
        
        from azure.eventhub import EventData
        from azure.eventhub.aio import EventHubProducerClient
        from azure.identity.aio import DefaultAzureCredential
        
        EVENT_HUB_FULLY_QUALIFIED_NAMESPACE = "EVENT_HUB_FULLY_QUALIFIED_NAMESPACE"
        EVENT_HUB_NAME = "EVENT_HUB_NAME"
        
        credential = DefaultAzureCredential()
        
        async def run():
            # Create a producer client to send messages to the event hub.
            # Specify a credential that has correct role assigned to access
            # event hubs namespace and the event hub name.
            producer = EventHubProducerClient(
                fully_qualified_namespace=EVENT_HUB_FULLY_QUALIFIED_NAMESPACE,
                eventhub_name=EVENT_HUB_NAME,
                credential=credential,
            )
            print("Producer client created successfully.") 
            async with producer:
                # Create a batch.
                event_data_batch = await producer.create_batch()
        
                # Add events to the batch.
                event_data_batch.add(EventData("First event "))
                event_data_batch.add(EventData("Second event"))
                event_data_batch.add(EventData("Third event"))
        
                # Send the batch of events to the event hub.
                await producer.send_batch(event_data_batch)
        
                # Close credential when no longer needed.
                await credential.close()
        
        asyncio.run(run())

1.	Primero creamos una carpeta del proyecto y lo vinculamos a VSCode.


![image](https://github.com/user-attachments/assets/43d0e973-94ae-41fb-abe2-5242a8c0dbc3)
<br><br>

![image](https://github.com/user-attachments/assets/66f147bb-0929-456e-9651-6e28305de9e7)

NOTA: Debemos tener pre instalado Git.

Ahora, instalamos uv para python.

Código:

        pip install uv
        
        ó
        
        python -m pip install uv


![image](https://github.com/user-attachments/assets/2457af2d-ad1a-4af1-913c-b4e4cd727fd3)

Código:

        uv init

y veras la creación de nuevos archivos en tu proyecto.


![image](https://github.com/user-attachments/assets/e2caaed0-cf07-4aa5-b777-6066afb301fa)

Luego, clone mi repositorio para cargar la carpeta con los datos.

Código:
	
        git clone https://github.com/AllGoHer/Uber_Data_Engineer_Project.git


![image](https://github.com/user-attachments/assets/86706baf-fa3b-47c4-88e5-65ad8dfd56b4)

Ahora, abrimos esa subcarpeta en VSCode.

![image](https://github.com/user-attachments/assets/b7666fea-8394-435e-86c0-ad6290132696)

NOTA: verificar que el archivo files_array.json quede de la siguiente manera.

![image](https://github.com/user-attachments/assets/087ea2aa-ca18-4ce6-a27d-f731cf568015)

Al igual, que el archivo clonado.

![image](https://github.com/user-attachments/assets/07b3f3be-cf84-418e-95e8-56c24b27397d)

Y finalmente guardamos los cambios.

En esta instancia, crearemos un entorno virtual y lo activamos.

Código:

        python -m venv .venv


Código:

        .venv/Scripts/activate


![image](https://github.com/user-attachments/assets/fdd164ac-7f14-4df9-a00f-25fa9fcb7659)

Luego, instalamos los requerimientos.

Código:

        pip install -r requirements.txt 


![image](https://github.com/user-attachments/assets/27ab934c-7ca9-4ace-8f37-2decf483dd6b)

Bien, ahora nos vamos Azure a enviar directivas (SendPolicy) y copiamos la clave de la cadena de conexión primaria.

![image](https://github.com/user-attachments/assets/0fde47a1-dcfe-4b3a-8d1a-cb42393f40fd)

Luego, regresas a VSCode y creamos el archivo .env y dentro de ella pegamos el código copiado de la siguiente manera.

1.	Creamos el archivo .env

2.	Escribimos el siguiente código antes de pegar la cadena de conexión principal copiada.

Código:

        CONNECTION_STRING = “            “   

3.	Dentro de las comillas pegamos la cadena de conexión principal copiada.


![image](https://github.com/user-attachments/assets/ca7aabde-ad8b-4a27-aca8-423485fa1ae5)

4.	Ahora complementamos el código de .env

Código:

		EVENT_HUBNAME = "ubertopic"
		EVENT_HUB_NAMESPACE = "eventosuber"


![image](https://github.com/user-attachments/assets/4fe12c99-1718-4ab6-9f59-2d77ea0d371a)

5.	Y finalmente guardamos el archivo.

Ahora, nos vamos al archivo conection.py y lo ejecutamos.


![image](https://github.com/user-attachments/assets/474e5a22-9937-44a8-b6ab-e8bea1f8c534)

Luego, nos vamos a Data Explorer para ver los eventos.

![image](https://github.com/user-attachments/assets/b7078473-0dad-4f05-ad69-c266cb99a2eb)

Luego, pasamos a la api.py y pediremos que se recargue en el localhost

Código:

        uvicorn api:app --reload


![image](https://github.com/user-attachments/assets/d6ba8764-9d67-4a5c-a838-9ddedfad38ab)
<br><br>

![image](https://github.com/user-attachments/assets/58e22d93-e737-414f-a011-45011a6d9e90)

Aquí haremos una reserva de viaje haciendo click en Book a Ride

![image](https://github.com/user-attachments/assets/03d68efc-92bb-4c77-b1b6-630578336d0f)

Ahora, en Azure veremos en el ubertopic otro evento.

![image](https://github.com/user-attachments/assets/7203a913-e6a2-40d1-b25d-798353c7d1d9)

<br><br>
____________________________________________________________________________________________________________________________________________________________________________________________________________________________
**AZURE DATA FACTORY**

![image](https://github.com/user-attachments/assets/46ef86bd-7bed-4520-b1d7-24f276903c89)

Nos vamos al portal azure e ingresamos a gestión de recursos/grupos de recursos/RG-UberProject y hacemos click en crear.

![image](https://github.com/user-attachments/assets/9ada65bc-a079-443b-84bf-f158055777e0)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/e7e09675-bef5-4486-b8a4-3e257957adf6)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/7b47e487-9f84-49cd-93ae-29e47c77c954)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/64a568be-ae33-4a37-88f3-d3cfb2c77617)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/019655bb-0422-47ce-b240-bd8f1eb45aed)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/78dd8e97-4209-4e76-9f0a-9310eaf30bb5)

Luego regresamos a grupos de recursos/RG-UberProject.

![image](https://github.com/user-attachments/assets/0a8a6306-638e-44bd-99a2-39c01470aa1b)


Y creamos un nuevo recurso y esta vez será un lago de datos.

![image](https://github.com/user-attachments/assets/dc21ae47-f0a8-4d65-aadd-357a0dd62590)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/f876a831-7de0-4c04-af56-757dc9005a23)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/66e658c8-8217-4722-970d-c84b37df1b54)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/73996205-dde7-4ffa-97de-228dc2aa1a35)

Luego damos click en crear.

Ahora veremos creado el lago de datos.

![image](https://github.com/user-attachments/assets/e0338e3f-b27c-4040-8b0e-ad03059b4e94)

Ahora, abriremos Azure Data Factory.

![image](https://github.com/user-attachments/assets/c45932c0-398e-4e1e-a767-e16c53629289)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/c0c58649-f5c2-44db-9958-1e71aeb5c741)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/75245fb6-a3d0-4348-bf58-c774ae72fa4a)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/9da76009-991e-4660-8df5-832a10c10d5d)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/9f35bf83-e9b8-447f-82c2-bf7033f041a6)

Ahora, en el recuadro de actividades escribimos copiar y arrastramos al panel central.

![image](https://github.com/user-attachments/assets/3a67b100-30b4-460e-a182-03fe0589bba6)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/e86f4d48-f4bd-408e-aaed-8446837a0759)

* Ahora, crearemos la conexión de GitHub con ADF
  
![image](https://github.com/user-attachments/assets/289e31ce-4587-4810-8c66-f7bc56a2aa1a)

Se puede hacer la conexión directa con github, pero como nuestro consumidor es un HTTP lo haremos a través de ella.

![image](https://github.com/user-attachments/assets/01afcb02-6552-426a-8321-e4cc84cfe5fa)

Luego, completaremos los datos solicitados

![image](https://github.com/user-attachments/assets/5b555db9-9d52-41f8-8db8-57fa3c5ffeea)

NOTA: Estos son los pasos para conseguir la URL Base.

1.	Vamos al github que clonamos y, entramos al archivo Data y seleccionamos uno de los archivos map.


![image](https://github.com/user-attachments/assets/0bbdc88b-39cf-4db6-b43f-4b681c9c7133)

2.	Luego, hacemos click en Raw.
   
![image](https://github.com/user-attachments/assets/d357e2d0-f332-4ca8-9875-53f70b33250b)

3.	Y copiamos la siguiente sección del URL para pegarlo en la URL Base.
   
![image](https://github.com/user-attachments/assets/adbd7ce4-d0ea-488a-b73e-e06cf0fd2822)

Luego de llenar los datos solicitados obtendremos lo siguiente.

![image](https://github.com/user-attachments/assets/c6c86579-4986-49e2-8959-b88067448f11)

Ahora, crearemos una conexión para el servicio con mis lagos de datos.

![image](https://github.com/user-attachments/assets/f91699e1-066f-4c9e-8a94-28f5368eb4cd)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/de66ee48-f2b2-4b6d-929e-fed3ff9d5a88)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/33be0751-7136-410a-aeb5-b53a9f020e6a)

* Ahora, crearemos un conjunto de datos.

Regresamos al Autor y entraremos a Datasets 


![image](https://github.com/user-attachments/assets/2ae57a5d-155e-42f9-909a-e21dc441cdd7)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/c847d5a9-5b2a-44cd-8985-fdd48242381b)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/6ea6b8e2-540a-4fe2-a209-d63b842e11f7)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/37062d2e-0e57-4b8a-80ba-0f72b94ee710)

Completamos la solicitud de los datos

![image](https://github.com/user-attachments/assets/5bbee367-0ac8-4d9b-a804-f64fcb4ecb30)

En la parte de Dirección de URL relativa es la parte complementaria de la URL Base anterior.

![image](https://github.com/user-attachments/assets/11c10b3d-6e5f-4b14-9c8d-5e5bba2879d1)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/61012a37-4051-486e-bc8b-d4f5b2b988b7)

Como han visto, hemos vinculado un set de datos pero el git hub tiene más el cual necesitaremos, pero también sabemos que todas las las url son casi las mismas y solo cambia el final. Entonces, para ello crearemos los parámetros 

![image](https://github.com/user-attachments/assets/de8e89e7-8004-4c20-8404-5038faa0ba3a)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/0a890ffa-2b36-446c-8d8b-c1487a1cb059)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/622d1923-274a-4889-b847-0e05db6005fa)

Regresamos a conexión y hacemos click en el recuadro dirección URL relativo y luego, hacemos click en la parte inferior en las letras azules que indican agregar contenido dinámico. 

![image](https://github.com/user-attachments/assets/c940a96a-3b4b-4477-9878-7a70edba12a0)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/19dc40a0-a5f0-4b87-950e-66338758fa30)

Dentro del primer recuadro colocamos la url complementaria como en el paso anterior.

![image](https://github.com/user-attachments/assets/d21f123f-66c4-4610-9e10-550a7f6c70e2)

Debe quedar hasta data, así. 

![image](https://github.com/user-attachments/assets/b6303d5b-e9eb-41ba-a58e-e6653f35ffc5)

Luego, agregaremos el parámetro p_file haciendo click en ella.

![image](https://github.com/user-attachments/assets/07c2b6fd-0602-4930-8b7b-78ef126d6bd4)

Luego, borramos el arroba que iba adelante y, agregamos un arroba después de Data/ y, encerramos entre llaves el dataset. Debe quedar de la siguiente manera.

![image](https://github.com/user-attachments/assets/fdfdbe43-cb69-43c5-93fa-7cc8d406af8e)

NOTA: Extiende el recuadro del generador de expresiones de canalización y verifica que no haya ningún espacio entre el @rroba y las llaves del dataset() para no tener problemas futuros en la depuración de la canalización. 

Y finalmente, damos aceptar.


Escribimos el siguiente código.

Código:

		[
		{"file":"map_cities"},
		{"file":"map_cancellation_reasons"},
		{"file":"bulk_rides"},
		{"file":"map_payment_methods"},
		{"file":"map_ride_statuses"},
		{"file":"map_vehicle_makes"},
		{"file":"map_vehicle_types"}
		]

Y ahora volvemos a la canalización HTTPToADLS y nos ubicamos en parámetros y crea uno nuevo. En él, asignamos un nombre (file_array) de tipo Matriz y en valor predeterminado pegamos los datos copiados.

![image](https://github.com/user-attachments/assets/9e59ca57-3194-468c-8942-a624aac90a8c)

Regresamos a HTTPToADLS y en actividades escribimos búsqueda y lo arrastramos al lienzo.

![image](https://github.com/user-attachments/assets/db160a75-56d9-4eed-9686-999f797db34d)

Ahora, en generales en la sección de nombre escribimos files_array o ds_files_array

![image](https://github.com/user-attachments/assets/b123f09a-8bc6-4772-8ad5-d646d937bfc6)

Luego, vamos a configuraciones y desmarcamos el check que está en solo la primera fila. 

![image](https://github.com/user-attachments/assets/3fba34c7-70b0-4fe6-85cc-fbc73a570272)

Debe quedar así.

![image](https://github.com/user-attachments/assets/5f03ec4d-69b8-492f-9852-5d041c17260c)

Luego, vamos al portal de Azure y hacemos los siguientes pasos:

1.	Ir a dlproyectouberdev/ almacenamiento de datos / contenedores.

![image](https://github.com/user-attachments/assets/faaa6cd7-edc7-4b31-9059-a8bdc20be5fa)

2.	Agregamos un nuevo contenedor.
   
![image](https://github.com/user-attachments/assets/75336dbc-2b1a-4930-806f-a81738e6ef3e)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/5d4e4940-f6a1-45e6-8ea0-f7ad27a4bd82)

3.	Ingresamos al archivo raw y cargamos el archivo files_array.json (tenerlo previamente descargado del github).
   
![image](https://github.com/user-attachments/assets/bcd609ae-b082-4914-9afe-d9b1408c8666)

4.	Regresamos a ADF-ProyectoUber-dev, hacemos click en búsqueda e ingresamos a configuración y hacemos click en nuevo.
   
![image](https://github.com/user-attachments/assets/4f82c843-29fa-437d-8cf1-2da123c96edc)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/96b94a51-52bb-4d36-ab13-410c4401655a)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/9a0d87e4-2bae-4b02-9596-595774117188)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/4a4de4da-2b78-432a-8121-3a2fac8ab0bd)

5.	Hacemos click en la carpeta y seleccionamos raw luego, files array.json y damos click en aceptar.
   
![image](https://github.com/user-attachments/assets/ae15cb58-b9c8-4fee-a5bc-fe32d34be77e)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/025d7789-9dcd-44c8-86a3-91a3d1f8d87a)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/b5e60b41-2c81-4c24-b515-2b3101588c5c)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/67ce2b28-ea20-4a90-a357-e0a30f726add)

* Ahora, en el lienzo seleccionamos copiar datos y en generales desactivamos el estado de la actividad.

![image](https://github.com/user-attachments/assets/d9c69db4-cb1a-401c-9c1e-e56f00af6a43)


Y hacemos click en depurar

![image](https://github.com/user-attachments/assets/99d1d05d-35f8-43f4-b335-4d111624df4b)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/61c2904a-ff60-43f9-9fc5-9663aec85ebb)

Ahora, verificamos el estado de salida.

![image](https://github.com/user-attachments/assets/e37e859a-272d-40b6-afd8-6708c7d58cfc)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/4e0d579f-e43b-47b3-8005-04638c27a887)

Ahora, en el casillero de actividades buscamos ForEach y lo arrastramos al lienzo.

![image](https://github.com/user-attachments/assets/e7a59a97-0194-4db6-a569-7e84689d7b2e)

Luego, cambiamos de nombre de ForEach1 a ForEachFile

![image](https://github.com/user-attachments/assets/aad7e7d2-b651-4375-8db5-8358b42f0140)

Ahora enlazamos los nodos de búsqueda con ForEachFile.

![image](https://github.com/user-attachments/assets/e5065cc6-b2f0-4ae9-9427-c4a61dc5342c)

Luego, seleccionamos ForEach y, nos vamos a configuración y hacemos click en el recuadro de elementos para que se active el texto de agregar contenido dinámico, en el cual haremos click.


![image](https://github.com/user-attachments/assets/089d2d80-b585-4e93-828e-c64c8f44100a)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/1bd05851-7dd9-4a7a-bf27-75236afe46fe)

Hacemos click en ds_files_array

![image](https://github.com/user-attachments/assets/90974b77-6591-4a6f-b346-305ec0c47883)

Y al final de la expresión agregamos .value

![image](https://github.com/user-attachments/assets/0990ae4f-6cf3-4665-9496-111956d22c20)

Ahora, seleccionamos copiar datos (HTTP_Ingestion) y lo cortamos para pegarlo dentro de ForEach. Luego, hacemos click en el lapiz (editar) de ForEach.


![image](https://github.com/user-attachments/assets/89c463b8-b935-4c18-ae37-f7e75e13f0a3)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/ef8c432a-d37a-490c-824b-289fa2f91af1)

Y aquí dentro pegamos copiar datos.

![image](https://github.com/user-attachments/assets/76fdd652-7f85-4723-a954-fcc025e60552)

Después, hacemos click en activado.

![image](https://github.com/user-attachments/assets/95941306-9f74-4c40-becc-c90359ce1a6b)

Como queremos utilizar esta actividad varias veces, entonces nos iremos a origen y en conjunto de datos de origen seleccionamos ds_github

![image](https://github.com/user-attachments/assets/ff125455-fd81-4379-89b5-26d4e02c7fb7)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/003828cc-21f4-4d0f-a366-44c7694b1824)

Ahora, pasamos el parámetro.


Entonces, haremos click dentro del recuadro y se activará bajo del recuadro con letras azules “agregar contenido dinámico”, en el cual, haremos click.

![image](https://github.com/user-attachments/assets/3f6b0ab9-a46f-4157-b48e-8c9517669db5)

Luego, en la ventana emergente pasamos el siguiente código.

Código:

        @item().file

![image](https://github.com/user-attachments/assets/3bcab709-a2d2-41db-b89a-c542ee3b89e3)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/1b1e2caf-5a00-4c98-a669-f762f566f008)

Ahora, nos vamos a Receptor y hacemos click en nuevo.

![image](https://github.com/user-attachments/assets/e0d488d2-8945-459a-bce0-7e497767aa07)

Seleccionamos Azure Data Lake Store Gen2.

![image](https://github.com/user-attachments/assets/1414c4b9-0af9-4949-a88f-339c1a862018)

Luego json

![image](https://github.com/user-attachments/assets/ee459bff-df84-488e-88c7-1c1ad8f5dbce)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/60afb34d-aac1-4ccc-8e35-1cba1191c5bc)

Luego hacemos click en abierto.

![image](https://github.com/user-attachments/assets/475f1b42-08f3-4516-ada4-3ee9e5a52951)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/5618ea68-3993-4410-a5cc-e98ce2d00b43)

Luego, vamos a parámetros y hacemos click en nuevo.

![image](https://github.com/user-attachments/assets/bda2504f-8d6a-4af3-9d24-86ca0e27904d)

![image](https://github.com/user-attachments/assets/87f3b4f0-524f-40a2-b938-48b9df70cbf5)

Hacemos click en el recuadro “nombre de archivo” y luego click en el texto agregar contenido dinamico.

![image](https://github.com/user-attachments/assets/78edd8df-39a9-4d44-85d4-b74f3b71e48f)

Luego, generamos la expresión de canalización.

Código:

        @dataset().p_file

![image](https://github.com/user-attachments/assets/7009fced-80a4-4ea5-9c57-78770cb92482)

Ahora, agregamos las llaves y punto json.

![image](https://github.com/user-attachments/assets/57bd1f70-fe1d-4971-8e42-c4f636f07e28)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/4b428ff4-5f7a-4595-891c-8cc8e0ea1adf)

Ahora, regresamos a HTTPToADLS y en Receptor, hacemos click en el recuadro de valor y luego click en agregar contenido dinámico.

![image](https://github.com/user-attachments/assets/0fb35d43-bb2e-4a7d-8550-673c126ab324)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/ccdcfe10-d056-47d7-b34d-5e2ac7d9ea39)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/684424ba-d61c-49e0-98b6-eb67442a7a61)

Ahora, regresamos a HTTPToADLS

![image](https://github.com/user-attachments/assets/44bbbe0b-e489-43d9-b810-05fb9f482454)

Luego, hacemos click en publicar

![image](https://github.com/user-attachments/assets/8c9c7205-c4df-4a30-9eef-0275449dc77a)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/ca2a2af6-25a1-437f-acca-ad927e8fee7b)

Ahora, volvemos a hacer click en depurar.

![image](https://github.com/user-attachments/assets/9e479db5-f66b-42f4-a224-0b9dda3fffdd)

Luego en aceptar.

![image](https://github.com/user-attachments/assets/72c2841d-0fb5-44e5-b0d3-8b474745a5e4)

Ahora podremos ver si hay errores.

![image](https://github.com/user-attachments/assets/082383f0-e8c1-4519-9b88-7c2628751536)

Veremos cual es problema del error.

![image](https://github.com/user-attachments/assets/38786f55-9848-44bd-b07c-68cfbb3c2ffd)

Entonces vemos que error es de conexión con HTTP.

Hacemos click en HTTP_Ingestion del ForEach y vamos al Receptor 

![image](https://github.com/user-attachments/assets/3f324fc6-bec5-4ba4-affc-60625d743123)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/3f9b1d3a-b6ae-4227-801c-a70348df41be)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/33ae70a2-1507-476d-83b0-4dd46a9b1a4c)

En la expresión agregamos las llaves y el punto json como el anterior.

![image](https://github.com/user-attachments/assets/d412bac4-ece4-4997-b173-837f2f0206cf)

Luego, publicamos y depuramos nuevamente.

![image](https://github.com/user-attachments/assets/d4951a45-a07b-4dbb-95eb-dfe48d3b0bc0)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/9acc1c16-788d-4bf4-939e-07657bb8204f)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/6a8c6b91-0228-49be-b888-2e0ff8a0a882)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/ca106c3e-2a75-492a-b1d6-06988ceb6a84)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/5cb25ce0-8d36-4ef6-aa07-5aa4ef1ef302)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/afe57b79-7837-4cea-9920-6f83d3c86b58)

Sale nuevamente error porque activamos el proceso antes de guardarlo. Entonces haremos click en HTTP_Ingestion y luego en Origen.


![image](https://github.com/user-attachments/assets/832fd499-150f-4ad9-93d6-9350ff1d24f0)

Y agregaremos las llaves y punto json.

![image](https://github.com/user-attachments/assets/1ebf787a-e13f-4e46-8834-8cb1b42e8cac)

Publicamos nuevamente.

![image](https://github.com/user-attachments/assets/923c3568-3d7b-4a20-affd-8a8744633eff)

Y depuramos.

![image](https://github.com/user-attachments/assets/d5ffca3e-d176-47c3-bc96-4391a8007fa7)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/49e461ad-c099-4f31-be26-d622e1081d97)

Para verificar que todo salió bien, vamos al portal Azure en contenedores / raw y debemos ver que se ha generado la carpeta ingestión.

![image](https://github.com/user-attachments/assets/a74e1f14-5103-4747-98e6-8e37bdb11ef9)

Hacemos click en ingestión.

![image](https://github.com/user-attachments/assets/27497ce9-c6a7-4c81-b032-da07da887abd)

* ahora, nos vamos a Databrick y crearemos un espacio de trabajo

![image](https://github.com/user-attachments/assets/9f9def6b-3533-4236-83df-a0aadc281586)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/08c7895c-71f4-4a8c-8bad-5dd43c0e3852)

Luego dentro de Uber_Project creamos un ETL Pipeline.

![image](https://github.com/user-attachments/assets/83ca67a3-5192-4cc7-a7d9-dbb062a1ecae)

Ahora, cambiaremos el nombre del pipeline, haciendo click en la pestaña y ponemos el nombre de uber_rides_ingestion.

![image](https://github.com/user-attachments/assets/11cea8ff-47dc-4a5c-a5be-1ade031b75ec)

Luego crearemos un catálogo, para ello haremos un duplicado de la pestaña de Databricks 

![image](https://github.com/user-attachments/assets/4f65dcdf-62a1-4d62-a9b7-e18b60dd3d0b)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/feb795b0-a365-4803-a9a6-ec531d2d3917)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/c510ab82-230f-4bc1-a9b3-20d8c21ab43a)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/b9c0f671-de57-48b3-88bf-86cddc1795c1)

Activamos All workspaces have access

![image](https://github.com/user-attachments/assets/4542444a-3176-40a3-9fe5-56a00b64ddf2)

Luego, damos click en siguiente.

![image](https://github.com/user-attachments/assets/84e03d82-62b0-46a0-b36a-44de33dbd16c)

Y luego en guardar.

![image](https://github.com/user-attachments/assets/6f4b556e-b4c2-407b-b121-a3b86d9239b7)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/49bb408e-6829-4de3-8d5a-d8d9e62e60f9)

Crearemos ahora un esquema llamado bronce

![image](https://github.com/user-attachments/assets/1722f09f-df94-4138-92c0-76cf855eafbe)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/6727bdf1-23af-48d8-87bc-16b63a456e4d)

Ahora regresamos a la otra pestaña del pipeline y configuramos Default Location haciendo click en workspace de la siguiente manera. 

![image](https://github.com/user-attachments/assets/4c4657d8-cbda-4e76-ad01-0309869b0c25)

Y luego guardamos.

![image](https://github.com/user-attachments/assets/3ca141ff-0c71-451d-9aae-5dec951a8c86)

Ahora, renombranos el archivo como ingest.py

![image](https://github.com/user-attachments/assets/984cc675-5879-46cf-839c-9679351739eb)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/2830a8a3-fb2d-4835-a230-43a14a0892bb)

Importamos las librerías necesarias.

Código:

		from pyspark import pipelines as dp
		from pyspark.sql.functions import *
		from pyspark.sql.types import *



![image](https://github.com/user-attachments/assets/fb74f8d8-04a0-41dc-b14c-8bd86388ce73)

Ahora buscaremos la estructura de configuración de eventos

![image](https://github.com/user-attachments/assets/02f103e9-5b2f-4cf2-8178-a7ca41fa903d)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/04cbe3c4-d771-4173-a797-77291b29d7d6)

Código:

		# Event Hubs configuration
		EH_NAMESPACE                    = spark.conf.get("iot.ingestion.eh.namespace")
		EH_NAME                         = spark.conf.get("iot.ingestion.eh.name")
		
		EH_CONN_SHARED_ACCESS_KEY_NAME  = spark.conf.get("iot.ingestion.eh.accessKeyName")
		SECRET_SCOPE                    = spark.conf.get("io.ingestion.eh.secretsScopeName")
		EH_CONN_SHARED_ACCESS_KEY_VALUE = dbutils.secrets.get(scope = SECRET_SCOPE, key = EH_CONN_SHARED_ACCESS_KEY_NAME)
		
		EH_CONN_STR                     = f"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={EH_CONN_SHARED_ACCESS_KEY_NAME};SharedAccessKey={EH_CONN_SHARED_ACCESS_KEY_VALUE}"
		# Kafka Consumer configuration
		
		KAFKA_OPTIONS = {
		  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
		  "subscribe"                : EH_NAME,
		  "kafka.sasl.mechanism"     : "PLAIN",
		  "kafka.security.protocol"  : "SASL_SSL",
		  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
		  "kafka.request.timeout.ms" : spark.conf.get("iot.ingestion.kafka.requestTimeout"),
		  "kafka.session.timeout.ms" : spark.conf.get("iot.ingestion.kafka.sessionTimeout"),
		  "maxOffsetsPerTrigger"     : spark.conf.get("iot.ingestion.spark.maxOffsetsPerTrigger"),
		  "failOnDataLoss"           : spark.conf.get("iot.ingestion.spark.failOnDataLoss"),
		  "startingOffsets"          : spark.conf.get("iot.ingestion.spark.startingOffsets")
		}

Luego, editamos el código, quedando de la siguiente manera.

Código:

		from pyspark import pipelines as dp
		from pyspark.sql.functions import *
		from pyspark.sql.types import *
		
		# Event Hubs configuration
		EH_NAMESPACE = "EventosUber"
		EH_NAME = "ubertopic"
		
		EH_CONN_STR = spark.conf.get("connection_string")
		# Kafka Consumer configuration
		
		KAFKA_OPTIONS = {
		  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
		  "subscribe"                : EH_NAME,
		  "kafka.sasl.mechanism"     : "PLAIN",
		  "kafka.security.protocol"  : "SASL_SSL",
		  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
		  "kafka.request.timeout.ms" : 10000,
		  "kafka.session.timeout.ms" : 10000,
		  "maxOffsetsPerTrigger"     : 10000,
		  "failOnDataLoss"           : "true",
		  "startingOffsets"          : "earliest"
		}
		
		@dp.table
		def rides_raw():
		    df = spark.readStream.format("kafka")\
		               .options(**KAFKA_OPTIONS)\
		               .load()
		
		    # Converting Values To string
		    df = df.withColumn("rides",col("value").cast("string"))
		
		    return df


Nos vamos a configuración

![image](https://github.com/user-attachments/assets/5b6ee0e0-a701-4e08-bb30-3412c3291e3e)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/be1a544f-6d7d-43cd-9d38-dc16b3c96695)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/7c5a4bff-6995-444d-a5c5-34faccbe1271)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/205bc7ae-6f1b-4c43-91ce-384fe914df30)

Ahora, creamos un folder llamado exploraciones y dentro de ella una archivo llamado exploraciones simples, donde pasaremos el código que creamos de la ingestión para hacer algunas pruebas.

![image](https://github.com/user-attachments/assets/fd787222-8ca7-4806-b97a-5c88357b88eb)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/92b1afd6-186e-45ae-89fb-1b94401c83c5)

Luego pegamos el código a dentro.

![image](https://github.com/user-attachments/assets/b44daa18-e9f4-4f48-a7b4-a1df7ad35a27)

Ahora, vamos a duplicar la pestaña de Databricks y en catalogo bronce crearemos un volumen.

![image](https://github.com/user-attachments/assets/84e6584d-de22-4bdd-a548-deac4fd77fb0)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/f80eb81e-3885-4bdd-88ff-c5a0929df90a)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/8dcf12b5-3f2b-4a2c-90a1-d269aee89121)

Ahora creamos un directorio.

![image](https://github.com/user-attachments/assets/a290ffcf-f1f1-4c9e-b9de-3444e9470926)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/c25f6a0c-315a-4895-9654-4b4abf09009b)

volvemos a ingest.py y ejecutamos el código. 

![image](https://github.com/user-attachments/assets/0e5f2c2c-fcf9-4cd0-8ae6-aca588d518f0)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/d64d858e-4db3-4718-bb33-7085c39150b9)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/87157bf8-5bfb-4239-bb09-8ec440821ea0) 
<br><br><br><br>

![image](https://github.com/user-attachments/assets/b3580d8b-0b4a-4204-a166-a3c5c944aafd)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/75fdaeab-edb5-4cf2-8adc-5d7036ca0dce)

Se necesitará subir estos datos a Databricks

![image](https://github.com/user-attachments/assets/5adece77-01b6-48f4-ae02-2bfe29466161)

Ahora, accederemos a los datos de raw / ingestión a Databricks. Para ello, iremos a tokens de acceso compartido en Azure.

![image](https://github.com/user-attachments/assets/d4e01edf-1b0e-4865-a2e9-0e8295c191b4)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/2dd63167-8089-4715-94a3-bf207f51188d)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/e4546bb6-2d59-485d-a61d-d836e491e4c5)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/40202a35-d629-46e3-81ea-8702b11722b6)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/4cfa1348-09d1-4e3c-861e-d626896a8df0)

En este caso del proyecto, solo solicitaremos permiso de lectura.

![image](https://github.com/user-attachments/assets/effbc989-a6ed-4d42-b5bf-703b9a175cdf)

Agregamos la fecha y hora de vencimiento y, generar URL y Token de SAS

![image](https://github.com/user-attachments/assets/0c72634c-73e5-4c15-83a5-1900fbfd5c42)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/e3eeef19-123e-4eaa-b3b7-300db3c54780)

También crearemos un token para subir de nivel el contenedor raw, haciendo el mismo procedimiento anterior y copiando el token SAS

<mark>NOTA:</mark> todos tus token y URL pégalos en un block de notas para no olvidarlos.

Ahora, vamos a Databricks a workspace del proyecto y, creamos un cuaderno llamado bronze_adls

![image](https://github.com/user-attachments/assets/1b576cc2-6503-4e26-bf2e-7f9c6709020c)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/e545f977-ccef-48ff-968c-6319f1e2c074)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/83195bfe-3942-4e84-b4b5-b64a4dde83ec)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/28de2c83-db33-44cc-8e2d-9ea85c3fc183)

importamos la librería pandas y la URL del proyecto más el archivo de ingestión/map_cities.json?.

<mark>Nota:</mark> el signo de interrogación “?” se usa para hacer un llamado a la API o solicitud de consulta.


Código:

		Import pandas as pd
		url = “https://dlproyectouberdev.blob.core.windows.net/raw/ingestion/map_cities.json.json?”

![image](https://github.com/user-attachments/assets/3f30fe08-13f3-46f0-9de3-fcfb6fc33eb4)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/bd2d2f4c-3064-4a08-af27-132847c99e10)

Luego agregamos al final de url el token de acceso de caducidad

![image](https://github.com/user-attachments/assets/77d0f394-c0a2-4a3f-bad2-f05a3ab282b5)


Ahora el dataframe de lectura de la url

Código:

		df = pd.read_json(url)
		df.head()


el código final de la celda.

Código:

		import pandas as pd

		url = "https://dlproyectouberdev.blob.core.windows.net/raw/ingestion/map_cities.json.json?sp=r&st=2026-09-27T00:40:49Z&se=2026-10-02T04:55:49Z&spr=https&sv=2026-02-06&sr=c&sig=8ugYaBCaq3s1YjwH1OUD6JeXZjUXgvrCEH12xiZVdzs%3D" 
		
		df = pd.read_json(url)
		df.head()



![image](https://github.com/user-attachments/assets/d9371127-5d0d-4968-8f51-213d97239d6a)

Ahora, parametrizaremos el código para poder trabajar con los diferentes archivos que tenemos en raw/ingestion

Código:

		import pandas as pd
		
		files = [
		{"file":"map_cities"},
		{"file":"map_cancellation_reasons"},
		{"file":"bulk_rides"},
		{"file":"map_payment_methods"},
		{"file":"map_ride_statuses"},
		{"file":"map_vehicle_makes"},
		{"file":"map_vehicle_types"}
		]
		
		for file in files:
		
		    url = f"https://dlproyectouberdev.blob.core.windows.net/raw/ingestion/{file['file']}.json.json?sp=r&st=2026-09-27T00:40:49Z&se=2026-10-02T04:55:49Z&spr=https&sv=2026-02-06&sr=c&sig=8ugYaBCaq3s1YjwH1OUD6JeXZjUXgvrCEH12xiZVdzs%3D" 
		
		    df = pd.read_json(url)
		    df_spark = spark.createDataFrame(df)
		
		    # Writing Data to the Bronze Layer
		    df_spark.write.format("delta")\
		        .mode("overwrite")\
		        .saveAsTable(f"uber.bronze.{file['file']}")


Y ejecutamos.


![image](https://github.com/user-attachments/assets/71c21e69-1c58-4dd9-8964-617ff6f85e5f)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/f93d101a-d815-4c8e-94b7-584196ad0093)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/f8809cf8-a539-4c07-8071-e8d773abe14a)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/a6a42740-9acb-48de-9efa-8f4aa0ae35bc)

Ahora probemos si todo va bien, para ello, haremos una consulta.

Código:

		SELECT * FROM uber.bronze.map_cities

![image](https://github.com/user-attachments/assets/42d08fa5-b5b8-4e00-9209-47f0672add5d)

Otra consulta.

Código:

		SELECT * FROM uber.bronze.bulk_rides

![image](https://github.com/user-attachments/assets/e4f776ff-65d8-4fef-97fc-0295196638c9)

Ahora, necesitamos unir la tabla bilk_rides con rides_raw de streaming para generar una tabla grande OBT en la capa de plata.

![image](https://github.com/user-attachments/assets/03325909-538c-4df3-a203-6d86ab649f80)

Ahora, nos vamos VSCode y ejecutamos el siguiente código.

Código:

		uvicorn api:app –reload

![image](https://github.com/user-attachments/assets/f5cefa32-70c2-48a5-a7de-f230e75645a7)

Hacemos control + click en el http y, luego hacemos click en book a ride

![image](https://github.com/user-attachments/assets/3394775f-2074-4d18-8871-d55f03a14c04)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/38447a3a-4679-4fdb-8c12-a7e12e7d7baa)
<br><br><br><br>

Esto confirma que se generó un nuevo evento.

![image](https://github.com/user-attachments/assets/c06aca78-f47b-4cd3-892f-731d65d3b191)
<br><br><br><br>

Ahora ejecutamos nuevamente ingest.py

![image](https://github.com/user-attachments/assets/4f056313-0f34-4ac0-bfab-32d5a4548dae)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/a8af5d8e-d2e1-40a7-a930-07f13187a011)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/a021e4b1-011f-4cc3-9c00-d89a8b11dd9f)
<br><br><br><br>

Ahora, verificaremos que todo vaya bien, vamos a bronze_adls y hacemos la siguiente consulta.

Código:

		SELECT * FROM uber.bronze.rides_raw


El resulta tendría que ser solo 5 filas.

![image](https://github.com/user-attachments/assets/9bb0825b-32a8-4ebc-b5fc-45a61d94ab2e)
<br><br><br><br>

___________________________________________________________________________________________________________________________
## SILVER LAYER
___________________________________________________________________________________________________________________________

![image](https://github.com/user-attachments/assets/39483532-2b13-48ad-b3a1-e8deb1609f63)

Primero, crearemos un archivo llamado silver.py

Código:

		from pyspark import pipelines as dp
		from pyspark.sql.functions import *
		from pyspark.sql.types import *
		
		# Empty Streaming Table
		dp.create_streaming_table("stg_rides")
		
		# Bulk/Initial Load
		@dp.append_flow(
		  target = "stg_rides"
		  ) 
		def rides_bulk():
		    df = spark.readStream.table("bulk_rides")
		    return df 
		
		# Streaming Load
		@dp.append_flow(
		  target = "stg_rides"
		  ) 
		def rides_stream():
		    df = spark.readStream.table("rides_raw")
		    return df


ejecutamos el código.

![image](https://github.com/user-attachments/assets/09af05e0-7ee7-48db-addf-c85510427a73)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/ea3681f0-6344-4190-8108-796f33869fed)
<br><br><br><br>

Ahora, crearemos notebook llamado silver_obt

![image](https://github.com/user-attachments/assets/fcdaa7c6-76b2-4186-bc46-f55a668a68d9)
<br><br><br><br>


Código:

		SELECT * FROM uber.bronze.stg_rides

![image](https://github.com/user-attachments/assets/ff6eb57e-d314-42f7-889b-aa3a01bb6334)

Bueno, ya probamos que está funcionando bien, pues ahora eliminaremos la tabla creada.

Código:

		DROP TABLE uber.bronze.stg_rides

![image](https://github.com/user-attachments/assets/5aaf2c18-f300-469a-89d0-3c8046ee31a1)

___________________________________________________________________________________________________________________________
### STREAM RIDES TRANSFORMETION

Código:

		df = spark.read.table("uber.bronze.rides_raw")
		display(df)

![image](https://github.com/user-attachments/assets/af08c989-8360-4bc8-a047-b970cf2b0a4d)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/9b5b8ffd-a848-4c9f-926b-9a2886b568a8)


Ahora, crearemos el esquema.

Código:

		df = spark.sql("SELECT * FROM uber.bronze.bulk_rides")
		df.schema


![image](https://github.com/user-attachments/assets/c10258cf-fc03-4f96-822c-41da6c245cd8)

Luego, vamos hacer algunos cambios en el orden del notebook stream rides.


Creamos una nueva celda en la parte superior 

![image](https://github.com/user-attachments/assets/eab30a19-226f-4586-bc55-f51aa4aca658)

Agregamos el siguiente código copiado de la estructura de la última celda

![image](https://github.com/user-attachments/assets/79d539d4-479f-40bc-8eb7-24a02bdd25ad)


Código:

		rides_schema = StructType([StructField('ride_id', StringType(), True), StructField('confirmation_number', StringType(), True), StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', LongType(), True), StructField('vehicle_make_id', LongType(), True), StructField('payment_method_id', LongType(), True), StructField('ride_status_id', LongType(), True), StructField('pickup_city_id', LongType(), True), StructField('dropoff_city_id', LongType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', LongType(), True), StructField('booking_timestamp', StringType(), True), StructField('pickup_timestamp', StringType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)])


![image](https://github.com/user-attachments/assets/0d986deb-58c6-47af-a530-cdc0d76e86fd)


Luego, creamos otra celda más en la parte superior para importar las librerías 

Código:

		from pyspark.sql.types import *
		from pyspark.sql.functions import *


![image](https://github.com/user-attachments/assets/5841d8bd-c047-4ab0-ad4d-0ab4aed35cc0)

En la siguiente celda

Código:

		df = spark.read.table("uber.bronze.rides_raw")
		df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema))
		display(df_parsed)

luego, ejecutamos todo hasta la cuarta celda.


![image](https://github.com/user-attachments/assets/f0e8ee1d-94ce-4710-96a2-fc59eb21108b)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/94602d6f-6197-4bb0-aeeb-25b572adad55)

Ahora, en la misma celda del código anterior, agregamos lo siguiente código para selccionar los parsed_rides por id.

![image](https://github.com/user-attachments/assets/62bc70dd-296f-4a8b-82e5-ee8f31f9b181)

Ahora, para ver todos los elementos se hará poniendo un asterisco después del parsed_rides.

Código:

		df = spark.read.table("uber.bronze.rides_raw")
		df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema)).select("parsed_rides.*")
		display(df_parsed)


![image](https://github.com/user-attachments/assets/6f26c739-6d29-43ff-a8af-394986918fab)


Ahora, copiamos todo el código de df_parsed y nos vamos a silver.py 

Código:

		df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema)).select("parsed_rides.*")


![image](https://github.com/user-attachments/assets/b18cfed9-ec08-4d12-8e31-0b02444ecb15)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/ca214a8f-caf8-4de7-8009-1e8ac0b274d7)


Luego copiamos rides_schema de silver_obt y lo pegamos bajo la importación de las librerías en silver.py

![image](https://github.com/user-attachments/assets/b1c80e4a-c835-4adb-9972-e7c38badf088)

Agregamos al final del código return df_parsed.

Y finalmente ejecutamos el pipeline.

![image](https://github.com/user-attachments/assets/2b737770-c91e-4195-a9bf-ba91131d2c4f)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/916f6638-73cf-42d8-8540-4636176dc123)

Ahora VSCode hacemos una nueva reserva (Book a Ride)

![image](https://github.com/user-attachments/assets/123b019a-613f-42d0-bdae-6628da830927)

Verificamos 

![image](https://github.com/user-attachments/assets/068e8887-4363-4796-81d1-5e26fba4a339)

Ejecutamos otra vez el pipeline silver.py.

![image](https://github.com/user-attachments/assets/79073f0c-f2fe-409f-919f-980507939893)

Volvemos ahora al notebook silver_obt y en una celda ejecutamos el siguiente código.

Código:

		SELECT * FROM uber.bronze.stg_rides

![image](https://github.com/user-attachments/assets/cbeb1420-b91b-4123-95a5-e78c6ff80f29)

____________________________________________________________________________________________________________________
#### JINJA TEMPLATE FOR OBT

Importamos jinja.

Código:

		pip install jinja2


![image](https://github.com/user-attachments/assets/b4d625d1-35b8-4097-99e3-1f72ffddd0c9)


sql:

		SELECT
		    stg_rides.*
		FROM
		    uber.bronze.stg_rides stg_rides
		LEFT JOIN
		    uber.bronze.map_vehicle_types map_vehicle_types
		ON
		    stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id
		    
		LEFT JOIN
		    uber.bronze.map_vehicle_makes map_vehicle_makes
		ON
		    stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id


![image](https://github.com/user-attachments/assets/4f7f5999-8ca0-43d0-b2d2-f1fbb5e7b2c3)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/9b2b50f3-d431-412a-b279-ee4f89339c6f)


Ahora, en una celda anterior al último ejecutado generamos la configuración jinja.

Código:

			jinja_config = [
			    {
			        "table" : "uber.bronze.stg_rides stg_rides",
			        "select" : "stg_rides.*",
			        "where" : ""
			    },
			    {
			        "table" : "uber.bronze.map_vehicle_makes map_vehicle_makes",
			        "select" : "map_vehicle_makes.vehicle_make",
			        "where" : "",
			        "on" : "stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id"
			    },
			    {
			        "table" : "uber.bronze.map_vehicle_types map_vehicle_types",
			        "select" : "map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,per_minute",
			        "where" : "",
			        "on" : "stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id"
			    }
			]


![image](https://github.com/user-attachments/assets/d76171e5-53cc-4d3d-b3ae-2b4f342b25e0)

Y ejecutamos el jinja_config

En la siguiente celda.

Código:

		from jinja2 import Template
		
		jinja_str = """
		
		    SELECT
		        {% for config in jinja_config %}
		            {{ config.select }}
		                {% if not loop.last %}
		                    ,
		                {% endif %}
		        {% endfor %}
		    FROM
		        {% for config in jinja_config %}
		            {% if loop.first %}
		                {{ config.table }}
		            {% else %}
		                LEFT JOIN {{ config.table }}
		                    ON {{ config.on }}
		            {% endif %}
		        {% endfor %}
		
		  
		        {% for config in jinja_config %}
		            
		            {% if loop.first %}
		                {% if config.where != "" %}
		                WHERE
		                {% endif %}
		            {% endif %}
		
		            {{ config.where }}
		                {% if not loop.last %}
		                    {% if config.where != "" %}
		                    AND
		                    {% endif %}
		                {% endif %}
		
		        {% endfor %}
		                   
		"""
		
		template = Template(jinja_str)
		rendered_template = template.render(jinja_config=jinja_config)
		print(rendered_template)



y ejecutamos.

![image](https://github.com/user-attachments/assets/9eadcb60-b15b-4ff9-b7d0-98a59de2fdab)

![image](https://github.com/user-attachments/assets/cdbd13a1-b47c-4ad4-a139-921c2ba69c3b)

En la siguiente celda.

Código:

		spark.sql(rendered_template)

![image](https://github.com/user-attachments/assets/a91bb865-a4b6-4ec8-8a33-ff6febf1694b)

En la misma celda modificamos

Código:

		display(spark.sql(rendered_template))


![image](https://github.com/user-attachments/assets/70a7309a-b11f-4d35-bae9-f4a042d9444c)


Ahora, completamos todo el código de jinja_config

Código:

			jinja_config = [
			    {
			        "table" : "uber.bronze.stg_rides stg_rides",
			        "select" : "stg_rides.*",
			        "where" : ""
			    },
			    {
			        "table" : "uber.bronze.map_vehicle_makes map_vehicle_makes",
			        "select" : "map_vehicle_makes.vehicle_make",
			        "where" : "",
			        "on" : "stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id"
			    },
			    {
			        "table" : "uber.bronze.map_vehicle_types map_vehicle_types",
			        "select" : "map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,map_vehicle_types.per_minute",
			        "where" : "",
			        "on" : "stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id"
			    },
			    {
			        "table" : "uber.bronze.map_ride_statuses map_ride_statuses",
			        "select" : "map_ride_statuses.ride_status",
			        "where" : "",
			        "on" : "stg_rides.ride_status_id = map_ride_statuses.ride_status_id"
			    },
			    {
			        "table" : "uber.bronze.map_payment_methods map_payment_methods",
			        "select" : "map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth",
			        "where" : "",
			        "on" : "stg_rides.payment_method_id = map_payment_methods.payment_method_id"
			    },
			    {
			        "table" : "uber.bronze.map_cities map_cities",
			        "select" : "map_cities.city as pickup_city, map_cities.state, map_cities.region",
			        "where" : "",
			        "on" : "stg_rides.pickup_city_id = map_cities.city_id"
			    },
			    {
			        "table" : "uber.bronze.map_cancellation_reasons map_cancellation_reasons",
			        "select" : "map_cancellation_reasons.cancellation_reason",
			        "where" : "",
			        "on" : "stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id" 
			    }
			]


![image](https://github.com/user-attachments/assets/5704f60a-fc23-4181-af00-d9fc5d2b1235)

![image](https://github.com/user-attachments/assets/34319c98-dc35-492b-8dc0-1b5c32725b22)

Y ejecutamos.

Luego, ejecutamos la siguiente celda de jinja_str


Ahora, en la siguiente celda pasamos el siguiente código.

Código:

		template = Template(jinja_str)
		rendered_template = template.render(jinja_config=jinja_config)
		display(spark.sql(rendered_template))



![image](https://github.com/user-attachments/assets/7fc04c93-31d4-47ab-8813-b92244736309)

Ahora, modificaremos el archivo map_cities.json  agregando     "updated_at": "2026-09-29T01:39:31.169+00:00" 

Código:
			
Ahora, modificaremos el archivo map_cities.json  agregando     "updated_at": "2026-09-29T01:39:31.169+00:00" 

Código:
		[
		  {
		    "city_id": 1,
		    "city": "New York",
		    "state": "NY",
		    "region": "Northeast",
		    "updated_at": "2026-09-29T01:39:31.169+00:00"
		  },
		  {
		    "city_id": 2,
		    "city": "Los Angelas",
		    "state": "CA",
		    "region": "West",
		    "updated_at": "2026-09-29T01:39:31.169+00:00"
		  },
		  {
		    "city_id": 3,
		    "city": "Chicago",
		    "state": "IL",
		    "region": "Midwest",
		    "updated_at": "2026-09-29T01:39:31.169+00:00"
		  },
		  {
		    "city_id": 4,
		    "city": "Houston",
		    "state": "TX",
		    "region": "South",
		    "updated_at": "2026-09-29T01:39:31.169+00:00"
		  },
		  {
		    "city_id": 5,
		    "city": "Phoenix",
		    "state": "AZ",
		    "region": "Southwest",
		    "updated_at": "2026-09-29T01:39:31.169+00:00"
		  },
		  {
		    "city_id": 6,
		    "city": "Philadelphia",
		    "state": "PA",
		    "region": "Northeast",
		    "updated_at": "2026-09-29T01:39:31.169+00:00"
		  },
		  {
		    "city_id": 7,
		    "city": "San Antonio",
		    "state": "TX",
		    "region": "South",
		    "updated_at": "2026-09-29T01:39:31.169+00:00"
		  },
		  {
		    "city_id": 8,
		    "city": "San Diego",
		    "state": "CA",
		    "region": "West",
		    "updated_at": "2026-09-29T01:39:31.169+00:00"
		  },
		  {
		    "city_id": 9,
		    "city": "Dallas",
		    "state": "TX",
		    "region": "South",
		    "updated_at": "2026-09-29T01:39:31.169+00:00"
		  },
		  {
		    "city_id": 10,
		    "city": "San Jose",
		    "state": "CA",
		    "region": "West",
		    "updated_at": "2026-09-29T01:39:31.169+00:00"
		  }
		]


![image](https://github.com/user-attachments/assets/4458015b-28eb-471d-bba5-e0aac10bb5af)

Guardamos los cambios y haremos un commit en git. Para ello, hacemos los siguiente:

Pasos para hacer un commit en VS Code

1.	Abre el Panel de Git: Haz clic en el icono de Control de código fuente en la barra lateral izquierda (tiene forma de bifurcación con tres círculos) o presiona las teclas Ctrl + Mayús + G.

2.	Prepara los cambios (Stage): Pasa el cursor sobre el nombre de tu archivo modificado (el que tiene la M) y haz clic en el botón con el icono de más (+). El archivo subirá a una sección llamada Cambios preparados.

3.	Escribe un mensaje: En la caja de texto que dice Mensaje, escribe una descripción breve de lo que hiciste (por ejemplo: Ajustar diseño de la cabecera).

4.	Confirma el Commit: Haz clic en el botón azul que dice Confirmar (o Commit).
¡Listo! La M desaparecerá porque Git ya guardó esa versión de tu archivo.


ahora nos dirigimos a ADF_ProyectoUber-dev y depuramos

![image](https://github.com/user-attachments/assets/4115e143-9788-479d-8210-9a3a9d9b7f56)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/d1b04820-add2-40b1-9fd5-be6ffa8709ec)


Ahora, regresamos al notebook bronce_adls y lo modificamos de la siguiente manera.

En la parte superior creamos una nueva celda para importar pandas.

Código:

		import pandas as pd

		df = pd.read_json("https://dlproyectouberdev.blob.core.windows.net/raw/ingestion/map_cities.json.json?sp=r&st=2026-09-27T00:40:49Z&se=2026-10-02T04:55:49Z&spr=https&sv=2026-02-06&sr=c&sig=8ugYaBCaq3s1YjwH1OUD6JeXZjUXgvrCEH12xiZVdzs%3D")
		
		df_spark = spark.createDataFrame(df)
		
		display(df_spark) 


y ejecutamos.

![image](https://github.com/user-attachments/assets/816412d3-65a9-4546-abe3-ff0f7d57f3d0)

Ahora, en la siguiente celda, hacemos las siguientes ediciones.

Código:

		import pandas as pd
		
		files = [
		{"file":"map_cities"},
		{"file":"map_cancellation_reasons"},
		{"file":"bulk_rides"},
		{"file":"map_payment_methods"},
		{"file":"map_ride_statuses"},
		{"file":"map_vehicle_makes"},
		{"file":"map_vehicle_types"}
		]
		
		for file in files:
		
		    url = f"https://dlproyectouberdev.blob.core.windows.net/raw/ingestion/{file['file']}.json.json?sp=r&st=2026-09-27T00:40:49Z&se=2026-10-02T04:55:49Z&spr=https&sv=2026-02-06&sr=c&sig=8ugYaBCaq3s1YjwH1OUD6JeXZjUXgvrCEH12xiZVdzs%3D" 
		
		    df = pd.read_json(url)
		    df_spark = spark.createDataFrame(df)
		
		    # Writing Data to the Bronze Layer
		    df_spark.write.format("delta")\
		        .mode("overwrite")\
		        .option("overwriteSchema", "true")\
		        .saveAsTable(f"uber.bronze.{file['file']}")


Y luego, ejecutamos.

En la siguiente celda seleccionaremos la tabla map_cities

Código:

		SELECT * FROM uber.bronze.map_cities


![image](https://github.com/user-attachments/assets/7c40eae7-6775-4fca-8db2-913036990517)

Luego en la siguiente celda pasamos el siguiente código.

Código:

		SELECT * FROM uber.bronze.rides_raw

![image](https://github.com/user-attachments/assets/8d961f52-1513-4eb2-a750-25f07c727ec4)

Ahora nos vamos al archivo silver.py y editamos el código, vamos a modificar StructField('booking_timestamp', StringType() a StructField('booking_timestamp', TimestampType()

A continuación, el código ya modificado.

Código:

		from pyspark import pipelines as dp
		from pyspark.sql.functions import *
		from pyspark.sql.types import *
		
		rides_schema = StructType([StructField('ride_id', StringType(), True), StructField('confirmation_number', StringType(), True), StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', LongType(), True), StructField('vehicle_make_id', LongType(), True), StructField('payment_method_id', LongType(), True), StructField('ride_status_id', LongType(), True), StructField('pickup_city_id', LongType(), True), StructField('dropoff_city_id', LongType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', LongType(), True), StructField('booking_timestamp', TimestampType(), True), StructField('pickup_timestamp', StringType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)])
		
		# Empty Streaming Table
		dp.create_streaming_table("stg_rides")
		
		# Bulk/Initial Load
		@dp.append_flow(
		  target = "stg_rides"
		  ) 
		def rides_bulk():
		    df = spark.readStream.table("bulk_rides")
		    df = df.withColumn("booking_timestamp", col("booking_timestamp").cast("timestamp")) 
		    return df 
		
		# Streaming Load
		@dp.append_flow(
		  target = "stg_rides"
		  ) 
		def rides_stream():
		    df = spark.readStream.table("rides_raw")
		    df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema)).select("parsed_rides.*")
		    return df_parsed
		    
		


ahora, nos dirigimos al notebook silver_obt
código:
drop table uber.bronze.stg_rides

![image](https://github.com/user-attachments/assets/277a7147-6d82-412f-a65c-c00efd8f81c2)

Luego regresamos a silver.py y ejecutamos el pipeline

![image](https://github.com/user-attachments/assets/daa2a21a-0ebc-4bf4-bcd6-4ddd9d46e157)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/0a5d4221-7527-40bb-a106-2b05f8a8b1e9)
<br><br><br><br>

![image](https://github.com/user-attachments/assets/80dede4f-b25e-47cc-89f2-af0a114b3414)

Ahora, regresamos a silver_obt y ejecutamos nuevamente

Código:

		SELECT * FROM uber.bronze.stg_rides

![image](https://github.com/user-attachments/assets/6d422f8e-1120-4d4f-ab54-c3ffe319dfbb)

En el archivo silver.py agregamos a las importaciones jinja2

Código:

		%pip install jinja2

![image](https://github.com/user-attachments/assets/e5a8b489-bf1c-4a06-b7b1-ac2b10d2c0c5)

Ahora, creamos en la carpeta de transformaciones el archivo silver_obt.sql

![image](https://github.com/user-attachments/assets/f8684cf7-fe89-4ba4-a4be-d1613b018307)

Pasamos el siguiente código:

Código:

		CREATE OR REFRESH STREAMING TABLE silver_obt
		AS 
		
		
		    SELECT 
		        
		            stg_rides.ride_id, stg_rides.confirmation_number, stg_rides.passenger_id, stg_rides.driver_id, stg_rides.vehicle_id, stg_rides.pickup_location_id, stg_rides.dropoff_location_id, stg_rides.vehicle_type_id, stg_rides.vehicle_make_id, stg_rides.payment_method_id, stg_rides.ride_status_id, stg_rides.pickup_city_id, stg_rides.dropoff_city_id, stg_rides.cancellation_reason_id, stg_rides.passenger_name, stg_rides.passenger_email, stg_rides.passenger_phone, stg_rides.driver_name, stg_rides.driver_rating, stg_rides.driver_phone, stg_rides.driver_license, stg_rides.vehicle_model, stg_rides.vehicle_color, stg_rides.license_plate, stg_rides.pickup_address, stg_rides.pickup_latitude, stg_rides.pickup_longitude, stg_rides.dropoff_address, stg_rides.dropoff_latitude, stg_rides.dropoff_longitude, stg_rides.distance_miles, stg_rides.duration_minutes, stg_rides.booking_timestamp, stg_rides.pickup_timestamp, stg_rides.dropoff_timestamp, stg_rides.base_fare, stg_rides.distance_fare, stg_rides.time_fare, stg_rides.surge_multiplier, stg_rides.subtotal, stg_rides.tip_amount, stg_rides.total_fare, stg_rides.rating 
		                
		                    ,
		                
		        
		            map_vehicle_makes.vehicle_make 
		                
		                    ,
		                
		        
		            map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,map_vehicle_types.per_minute 
		                
		                    ,
		                
		        
		            map_ride_statuses.ride_status 
		                
		                    ,
		                
		        
		            map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth 
		                
		                    ,
		                
		        
		            map_cities.city as pickup_city, map_cities.state, map_cities.region, map_cities.updated_at as city_updated_at 
		                
		                    ,
		                
		        
		            map_cancellation_reasons.cancellation_reason 
		                
		        
		    FROM 
		        
		            
		                STREAM (uber.bronze.stg_rides) 
		                WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES stg_rides
		            
		            
		                LEFT JOIN uber.bronze.map_vehicle_makes map_vehicle_makes ON stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id
		            
		        
		            
		                LEFT JOIN uber.bronze.map_vehicle_types map_vehicle_types ON stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id
		            
		        
		            
		                LEFT JOIN uber.bronze.map_ride_statuses map_ride_statuses ON stg_rides.ride_status_id = map_ride_statuses.ride_status_id
		            
		        
		            
		                LEFT JOIN uber.bronze.map_payment_methods map_payment_methods ON stg_rides.payment_method_id = map_payment_methods.payment_method_id
		            
		        
		            
		                LEFT JOIN uber.bronze.map_cities map_cities ON stg_rides.pickup_city_id = map_cities.city_id
		            
		        
		            
		                LEFT JOIN uber.bronze.map_cancellation_reasons map_cancellation_reasons ON stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id
            
        
Y ejecutamos el pipeline 2 veces.


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



