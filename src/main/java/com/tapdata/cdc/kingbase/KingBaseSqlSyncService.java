package com.tapdata.cdc.kingbase;

import com.tapdata.cdc.config.ApplicationProperties;
import com.tapdata.cdc.elasticsearch.ElasticsearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executes configured KingBase SQL statements and syncs results into Elasticsearch.
 * Each statement can optionally run incrementally by supplying the last sync timestamp
 * as the sole positional parameter ("?") and identifying a timestamp column whose
 * maximum value will be tracked between executions.
 */
@Service
@ConditionalOnProperty(prefix = "tap.kingbase", name = {"enabled", "sql-sync-enabled"}, havingValue = "true")
public class KingBaseSqlSyncService {

    private static final Logger logger = LoggerFactory.getLogger(KingBaseSqlSyncService.class);

    private final ElasticsearchService elasticsearchService;
    private final ApplicationProperties.KingBase kingBaseProperties;
    private final Map<String, Long> lastSyncedIds = new ConcurrentHashMap<String, Long>();
    private final AtomicBoolean manualTriggerRequested = new AtomicBoolean(false);
    private final Object syncMonitor = new Object();

    @Autowired(required = false)
    private JdbcTemplate kingbaseJdbcTemplate;

    public KingBaseSqlSyncService(ElasticsearchService elasticsearchService,
                                  ApplicationProperties applicationProperties) {
        this.elasticsearchService = elasticsearchService;
        this.kingBaseProperties = applicationProperties.getKingbase();
    }

    @PostConstruct
    public void logConfiguration() {
        if (kingbaseJdbcTemplate == null) {
            logger.info("KingBase SQL sync service initialized without JdbcTemplate; waiting for datasource configuration.");
        }

        List<ApplicationProperties.KingBase.SqlStatement> statements = kingBaseProperties.getSqlStatements();
        if (CollectionUtils.isEmpty(statements)) {
            logger.info("No KingBase SQL statements configured for sync.");
        } else {
            logger.info("Configured {} KingBase SQL statement(s) for Elasticsearch sync.", statements.size());
        }
    }

    @Scheduled(fixedDelayString = "${tap.kingbase.sql-sync-interval-ms:10000}")
    public void syncStatements() {
        synchronized (syncMonitor) {
            boolean triggeredManually = manualTriggerRequested.getAndSet(false);

            if (kingbaseJdbcTemplate == null) {
                if (triggeredManually) {
                    logger.warn("Manual KingBase SQL sync requested but JdbcTemplate is not configured yet.");
                }
                return;
            }

            List<ApplicationProperties.KingBase.SqlStatement> statements = kingBaseProperties.getSqlStatements();
            if (CollectionUtils.isEmpty(statements)) {
                if (triggeredManually) {
                    logger.info("Manual KingBase SQL sync requested but no statements are configured.");
                }
                return;
            }

            int processedStatements = 0;

            for (ApplicationProperties.KingBase.SqlStatement statement : statements) {
                if (statement == null || !statement.isEnabled()) {
                    continue;
                }

                if (!StringUtils.hasText(statement.getSql())) {
                    logger.warn("Skipping SQL statement '{}' because SQL text is blank.", statement.getName());
                    continue;
                }

                try {
                    syncStatement(statement);
                    processedStatements++;
                } catch (Exception ex) {
                    logger.error("Failed to sync SQL statement '{}': {}", statement.getName(), ex.getMessage(), ex);
                }
            }

            if (triggeredManually) {
                logger.info("Manual KingBase SQL sync completed. Processed {} statement(s).", processedStatements);
            }

            // Ensure any queued write operations are flushed promptly when a sync finishes.
            elasticsearchService.flushPendingBulkOperations();
        }
    }

    /**
     * Allows callers to request an immediate sync outside the scheduled cadence.
     */
    public void triggerManualSync() {
        manualTriggerRequested.set(true);
        syncStatements();
    }

    private void syncStatement(ApplicationProperties.KingBase.SqlStatement statement) {
        String statementKey = resolveStatementKey(statement);
        long lastId = lastSyncedIds.getOrDefault(statementKey, 0L);

        List<Map<String, Object>> rows = executeStatement(statement, lastId);
        if (rows.isEmpty()) {
            logger.debug("No results for SQL statement '{}' since ID {}", statementKey, lastId);
            return;
        }

        long maxObservedId = lastId;
        boolean idObserved = false;

        for (Map<String, Object> row : rows) {
            Map<String, Object> document = sanitizeRow(row);
            document.put("_query", statementKey);
            document.put("_sync_timestamp", Instant.now().toString());

            String indexName = resolveIndexName(statement);
            String documentId = resolveDocumentId(statement, row, statementKey);

            elasticsearchService.indexDocument(indexName, documentId, document);

            Long rowId = extractId(row.get(statement.getIdColumn()));
            if (rowId != null) {
                idObserved = true;
                if (rowId > maxObservedId) {
                    maxObservedId = rowId;
                }
            }
        }

        if (statement.isIncremental()) {
            if (!idObserved) {
                logger.warn("SQL statement '{}' is marked incremental but ID column '{}' was missing or unparsable in the result set.",
                    statementKey, statement.getIdColumn());
            }

            long nextId = idObserved ? maxObservedId : lastId;
            lastSyncedIds.put(statementKey, nextId);
            logger.info("Synced {} row(s) for SQL statement '{}'; next sync cursor ID: {}", rows.size(), statementKey, nextId);
        } else {
            logger.info("Synced {} row(s) for SQL statement '{}' (non-incremental).", rows.size(), statementKey);
        }
    }

    private List<Map<String, Object>> executeStatement(ApplicationProperties.KingBase.SqlStatement statement,
                                                        long lastId) {
        try {
            if (statement.isIncremental()) {
                return kingbaseJdbcTemplate.queryForList(statement.getSql(), lastId);
            }
            return kingbaseJdbcTemplate.queryForList(statement.getSql());
        } catch (Exception ex) {
            logger.error("Error executing SQL statement '{}': {}", statement.getName(), ex.getMessage());
            return new ArrayList<Map<String, Object>>();
        }
    }

    private String resolveStatementKey(ApplicationProperties.KingBase.SqlStatement statement) {
        if (StringUtils.hasText(statement.getName())) {
            return statement.getName();
        }
        return Integer.toHexString(statement.getSql() != null ? statement.getSql().hashCode() : 0);
    }

    private String resolveIndexName(ApplicationProperties.KingBase.SqlStatement statement) {
        if (StringUtils.hasText(statement.getIndex())) {
            return normalizeIndexName(statement.getIndex());
        }
        return normalizeIndexName(resolveStatementKey(statement));
    }

    private String normalizeIndexName(String candidate) {
        String index = candidate.toLowerCase().replaceAll("[^a-z0-9_-]", "_");
        if (index.startsWith("_")) {
            index = "idx" + index;
        }
        return index;
    }

    private String resolveDocumentId(ApplicationProperties.KingBase.SqlStatement statement,
                                     Map<String, Object> row,
                                     String statementKey) {
        String idColumn = statement.getIdColumn();
        if (StringUtils.hasText(idColumn)) {
            Object idValue = row.get(idColumn);
            if (idValue != null) {
                return statementKey + "_" + idValue;
            }
        }

        int hash = 0;
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getValue() != null) {
                hash += (entry.getKey() + ":" + entry.getValue()).hashCode();
            }
        }

        return statementKey + "_" + Math.abs(hash);
    }

    private Map<String, Object> sanitizeRow(Map<String, Object> row) {
        Map<String, Object> sanitized = new HashMap<String, Object>(row.size());
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof java.sql.Timestamp) {
                sanitized.put(entry.getKey(), ((java.sql.Timestamp) value).toInstant().toString());
            } else if (value instanceof java.sql.Date) {
                sanitized.put(entry.getKey(), value.toString());
            } else if (value instanceof java.sql.Time) {
                sanitized.put(entry.getKey(), value.toString());
            } else {
                sanitized.put(entry.getKey(), value);
            }
        }
        return sanitized;
    }

    private Long extractId(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            Number number = (Number) value;
            return number.longValue();
        }

        if (value instanceof CharSequence) {
            CharSequence sequence = (CharSequence) value;
            try {
                return Long.parseLong(sequence.toString());
            } catch (NumberFormatException ignored) {
                logger.debug("Unable to parse ID value '{}' as long", sequence);
            }
        }

        return null;
    }
}
