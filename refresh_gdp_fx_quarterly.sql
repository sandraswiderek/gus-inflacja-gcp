CREATE OR REPLACE TABLE `zmiana-cen-i-inflacja-w-polsce.gdp_dataset.gdp_fx_quarterly_mt`
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2010, 2100)) AS
WITH fx AS (
  SELECT
    EXTRACT(YEAR FROM quarter) AS year,
    CONCAT("Q", CAST(EXTRACT(QUARTER FROM quarter) AS STRING)) AS quarter,
    UPPER(currency) AS currency,
    avg_rate
  FROM `zmiana-cen-i-inflacja-w-polsce.gdp_dataset.kursy_kwartalne_ec2`
  WHERE UPPER(currency) IN ("EUR","USD")
),
fx_pivot AS (
  SELECT
    year, quarter,
    MAX(IF(currency="EUR", avg_rate, NULL)) AS value_eur,
    MAX(IF(currency="USD", avg_rate, NULL)) AS value_usd
  FROM fx
  GROUP BY year, quarter
),
gdp AS (
  SELECT
    year,
    quarter,
    value_mlrd AS value_gdp
  FROM `zmiana-cen-i-inflacja-w-polsce.gdp_dataset.gdp_mlrd`
  WHERE year >= 2010
)
SELECT
  gdp.year, gdp.quarter, gdp.value_gdp,
  fx_pivot.value_eur, fx_pivot.value_usd
FROM gdp
LEFT JOIN fx_pivot USING (year, quarter);
