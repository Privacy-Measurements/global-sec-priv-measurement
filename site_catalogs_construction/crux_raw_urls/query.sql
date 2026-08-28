SELECT
  DISTINCT origin,
  experimental.popularity.rank
FROM
  `chrome-ux-report.experimental.country`
WHERE
  country_code = 'in'
  AND yyyymm = YYYYMM
  AND experimental.popularity.rank <= 1000000
GROUP BY
  origin,
  experimental.popularity.rank
ORDER BY
  experimental.popularity.rank;

SELECT
  DISTINCT origin,
  experimental.popularity.rank
FROM
  `chrome-ux-report.experimental.country`
WHERE
  country_code = 'de'
  AND yyyymm = YYYYMM
  AND experimental.popularity.rank <= 1000000
GROUP BY
  origin,
  experimental.popularity.rank
ORDER BY
  experimental.popularity.rank;

SELECT
  DISTINCT origin,
  experimental.popularity.rank
FROM
  `chrome-ux-report.experimental.country`
WHERE
  country_code = 'ae'
  AND yyyymm = YYYYMM
  AND experimental.popularity.rank <= 1000000
GROUP BY
  origin,
  experimental.popularity.rank
ORDER BY
  experimental.popularity.rank;

SELECT
  DISTINCT origin,
  experimental.popularity.rank
FROM
  `chrome-ux-report.experimental.country`
WHERE
  country_code = 'us'
  AND yyyymm = YYYYMM
  AND experimental.popularity.rank <= 1000000
GROUP BY
  origin,
  experimental.popularity.rank
ORDER BY
  experimental.popularity.rank;



SELECT
  DISTINCT origin,
  experimental.popularity.rank
FROM
  `chrome-ux-report.experimental.global`
WHERE
  yyyymm = YYYYMM
  AND experimental.popularity.rank <= 1000000
GROUP BY
  origin,
  experimental.popularity.rank
ORDER BY
  experimental.popularity.rank;