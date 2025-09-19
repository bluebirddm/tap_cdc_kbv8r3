-- Query for T_JW_RYCW table
-- This SQL file can be maintained separately without escaping issues
SELECT *
FROM "XJ"."T_JW_RYCW"
WHERE "ID" > ?
ORDER BY "ID"
LIMIT :chunkSize