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
        git clone https://github.com/anshlambagit/Uber_Data_Engineer_Project.git
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

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🧠 DESARROLLO DEL PROYECTO Y PRUEBA VISUAL (VISUAL PROOF) 📸 
___________________________________________________________________________________________________________________________________________________________________________________________________________________________
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

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()
