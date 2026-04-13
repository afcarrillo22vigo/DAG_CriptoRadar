{{ config(materialized='table') }}

SELECT
    symbol,
    current_price,
    price_change_percentage_24h,
    "Date"
FROM criptos_oro
WHERE price_change_percentage_24h > 0
ORDER BY price_change_percentage_24h DESC
LIMIT 5