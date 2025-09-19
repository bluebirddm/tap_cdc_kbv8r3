-- Query for IDS_STR table
-- Complex SQL with multiple conditions
SELECT
    "ID",
    "NAME",
    "DESCRIPTION",
    "CREATED_AT",
    "UPDATED_AT",
    "STATUS"
FROM "IDS_STR"
WHERE "ID" > ?
  AND "STATUS" = 'ACTIVE'
ORDER BY "ID"
LIMIT :chunkSize