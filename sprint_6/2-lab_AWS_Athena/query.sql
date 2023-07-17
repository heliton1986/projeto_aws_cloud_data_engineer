SELECT
  decada,
  nomes
FROM (
  SELECT
    nomes,
    SUBSTRING(CAST(ano AS varchar), 1, 3) || '0s' AS decada,
    SUM(total) AS contagem_nomes,
    ROW_NUMBER() OVER (PARTITION BY SUBSTRING(CAST(ano AS varchar), 1, 3) || '0s' ORDER BY SUM(total) DESC) AS rn
  FROM
    meubanco.tbnome
    WHERE ano >=1950    
  GROUP BY
    nomes,
    SUBSTRING(CAST(ano AS varchar), 1, 3) || '0s'
)
WHERE
  rn <= 3
ORDER BY
  decada,
  contagem_nomes DESC;
