# Radar Financiero Cripto (ETL Pipeline)

Un pipeline de ingeniería de datos 100% automatizado que extrae, limpia y transforma datos del mercado de criptomonedas en tiempo real utilizando la arquitectura Medallón.

## Tecnologías Utilizadas
* **Orquestación:** Apache Airflow
* **Extracción & Transformación (Python):** Requests, Pandas, SQLAlchemy
* **Almacenamiento (Data Warehouse):** PostgreSQL
* **Modelado de Datos (Transformación):** dbt (data build tool)
* **Infraestructura:** Docker & Docker Compose

## Arquitectura del Pipeline

1. **Capa Bronce (Ingesta):** Airflow ejecuta una llamada a la API pública de CoinGecko para extraer el top 50 de criptomonedas por capitalización de mercado.
2. **Capa Plata (Limpieza):** Usando Pandas, se filtran monedas con valores anómalos (precio < 0), se seleccionan las métricas clave y se guarda un snapshot temporal.
3. **Capa Oro (Carga):** Los datos limpios se inyectan en una base de datos PostgreSQL.
4. **Capa Analítica (dbt):** dbt toma el control dentro del Data Warehouse para generar tablas agregadas con valor de negocio:
    * `top_5_ganadoras`: Las criptomonedas con mayor crecimiento en las últimas 24h.
    * `resumen_volumen`: Clasificación de monedas según su liquidez.

## Cómo ejecutar este proyecto en local

1. Clona este repositorio: `git clone <tu-enlace-aqui>`
2. Levanta la infraestructura con Docker: `docker-compose up -d`
3. Accede a Airflow en `http://localhost:8080` (usuario/contraseña por defecto en `docker-compose.yaml`).
4. Activa y ejecuta el DAG `analisis_criptomonedas_new`.
5. Conéctate al puerto `5432` con tu cliente SQL favorito para ver los resultados procesados por dbt.