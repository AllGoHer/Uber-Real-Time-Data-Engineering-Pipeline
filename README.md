# 🚕 Uber-Real-Time-Data-Engineering-Pipeline
___________________________________________________________________________________________________________________________________________________________________________________________________________________

![image](https://github.com/user-attachments/assets/e6fdecca-1b98-47c8-b0bd-e52c196daca8)

___________________________________________________________________________________________________________________________________________________________________________________________________________________

![image](https://github.com/user-attachments/assets/08770ee9-8e89-421c-a3f5-7ebb8433beb0) ![image](https://github.com/user-attachments/assets/aa7a98eb-3975-4e31-8f08-1e198a282ed5) ![image](https://github.com/user-attachments/assets/e7ea03f1-1202-48e1-97d1-daec815fc01d) ![image](https://github.com/user-attachments/assets/78b9d580-40e5-4561-bad7-db01a2540dcc) ![image](https://github.com/user-attachments/assets/285f7fe1-2f6d-4db5-b310-fc2334cbd4ce) ![image](https://github.com/user-attachments/assets/8a8f8346-b315-40ed-9c8f-54a68a27fd04)

## 🛠️ Nota de Arquitectura
Este proyecto fue diseñado, orquestado y desplegado al 100% desde cero. Se implementó un pipeline de streaming stateful utilizando Spark Structured Streaming con semántica Exactly-Once, procesando eventos de Uber en tiempo real a través de la Arquitectura Medallion (Bronze → Silver → Gold) sobre Azure.


## 🎯 Resumen Ejecutivo
Este proyecto simula y procesa la infraestructura de datos de Uber en tiempo real. Ingesta millones de eventos de telemetría (ubicaciones, solicitudes de viaje, pagos) a través de Azure Event Hubs, los procesa de forma resiliente con Azure Databricks (Spark Streaming), los gobierna con Unity Catalog, y los expone para análisis operativo en tiempo real.

Descripción del Proyecto
Este es un pipeline de datos en tiempo real de nivel producción que simula el flujo completo de datos de un sistema de reserva de viajes de Uber. Comienza cuando un usuario reserva un viaje a través de una Web App, los datos fluyen en tiempo real hacia Azure Event Hubs, se procesan con Databricks y Spark Declarative Pipelines, y finalmente se modelan en un STAR Schema listo para análisis.

Puntos clave: Arquitectura unificada de procesamiento de streaming en tiempo real + carga de datos históricos por lotes, con diseño de capas Medallion (Bronze → Silver → Gold) .

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()
