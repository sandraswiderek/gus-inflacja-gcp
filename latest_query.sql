CREATE OR REPLACE TABLE `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.kursy_latest` AS
SELECT t.*
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY currency ORDER BY date DESC) rn
  FROM `zmiana-cen-i-inflacja-w-polsce.bdl_dataset.kursy`
) t
WHERE rn = 1;
