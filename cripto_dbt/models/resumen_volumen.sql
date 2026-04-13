{{ config(materialized='table') }}

SELECT
    symbol,
    total_volume,
    CASE
        WHEN total_volume >= 1000000000 THEN 'VOLUMEN_ALTO'
        WHEN total_volume >= 100000000 THEN 'VOLUMEN_MEDIO'
        ELSE 'BAJO_VOLUMEN'
    END AS clasificacion_volumen, "Date"
FROM criptos_oro
ORDER BY total_volume DESC