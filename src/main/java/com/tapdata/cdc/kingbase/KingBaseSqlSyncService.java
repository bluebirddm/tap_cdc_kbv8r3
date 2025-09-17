package com.tapdata.cdc.kingbase;

import com.tapdata.cdc.config.ApplicationProperties;
import com.tapdata.cdc.elasticsearch.ElasticsearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final AtomicBoolean syncInProgress = new AtomicBoolean(false);
    private final ExecutorService statementExecutor;

    private volatile JdbcTemplate kingbaseJdbcTemplate;
    private volatile KingBaseSyncStateRepository stateRepository;

    public KingBaseSqlSyncService(ElasticsearchService elasticsearchService,
                                  ApplicationProperties applicationProperties) {
        this.elasticsearchService = elasticsearchService;
        this.kingBaseProperties = applicationProperties.getKingbase();
        this.statementExecutor = createStatementExecutor(kingBaseProperties.getStatementParallelism());
    }

    @Autowired(required = false)
    public void setKingbaseJdbcTemplate(JdbcTemplate kingbaseJdbcTemplate) {
        this.kingbaseJdbcTemplate = kingbaseJdbcTemplate;
        this.stateRepository = null;
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

    @PreDestroy
    public void shutdownExecutor() {
        statementExecutor.shutdown();
    }

    @Scheduled(fixedDelayString = "${tap.kingbase.sql-sync-interval-ms:10000}")
    public void syncStatements() {
        boolean triggeredManually = manualTriggerRequested.getAndSet(false);

        if (!syncInProgress.compareAndSet(false, true)) {
            if (triggeredManually) {
                logger.warn("Manual KingBase SQL sync requested but a previous run is still in progress.");
            } else {
                logger.debug("Skipping KingBase SQL sync because a previous run is still in progress.");
            }
            return;
        }

        try {
            JdbcTemplate template = kingbaseJdbcTemplate;
            if (template == null) {
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

            getStateRepository();

            List<Future<?>> futures = new ArrayList<Future<?>>();
            int submittedStatements = 0;

            for (ApplicationProperties.KingBase.SqlStatement statement : statements) {
                if (!isExecutable(statement)) {
                    continue;
                }

                futures.add(statementExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            runSingleStatement(statement);
                        } catch (Exception ex) {
                            logger.error("Failed to sync SQL statement '{}': {}", statement.getName(), ex.getMessage(), ex);
                        }
                    }
                }));
                submittedStatements++;
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted while waiting for KingBase SQL sync tasks to complete.");
                    break;
                } catch (ExecutionException ex) {
                    Throwable cause = ex.getCause();
                    if (cause != null) {
                        logger.error("KingBase SQL sync task failed: {}", cause.getMessage(), cause);
                    } else {
                        logger.error("KingBase SQL sync task failed: {}", ex.getMessage(), ex);
                    }
                }
            }

            if (triggeredManually) {
                logger.info("Manual KingBase SQL sync completed. Processed {} statement(s).", submittedStatements);
            }
        } finally {
            syncInProgress.set(false);
        }
    }

    /**
     * Allows callers to request an immediate sync outside the scheduled cadence.
     */
    public void triggerManualSync() {
        manualTriggerRequested.set(true);
        syncStatements();
    }

    private void runSingleStatement(ApplicationProperties.KingBase.SqlStatement statement) {
        String statementKey = resolveStatementKey(statement);
        long cursor = resolveLastSyncedId(statementKey);
        int chunkSize = resolveChunkSize(statement);
        int fetchSize = resolveFetchSize(statement, chunkSize);
        boolean incremental = statement.isIncremental() && StringUtils.hasText(statement.getIdColumn());
        String baseSql = statement.getSql();

        long processedRows = 0;
        long maxObservedId = cursor;
        boolean idObservedAny = false;

        while (!Thread.currentThread().isInterrupted()) {
            StatementChunkResult chunkResult = executeStatementChunk(statement, statementKey, baseSql, cursor, chunkSize, fetchSize, incremental);
            if (chunkResult.getProcessedRows() == 0) {
                break;
            }

            processedRows += chunkResult.getProcessedRows();

            if (incremental) {
                if (chunkResult.isIdObserved()) {
                    idObservedAny = true;
                    if (chunkResult.getMaxObservedId() > maxObservedId) {
                        maxObservedId = chunkResult.getMaxObservedId();
                        cursor = maxObservedId;
                        persistLastSyncedId(statementKey, cursor);
                    }
                } else {
                    logger.warn("SQL statement '{}' is marked incremental but ID column '{}' was missing or unparsable in the result set.",
                        statementKey, statement.getIdColumn());
                    break;
                }
            }

            if (chunkSize <= 0 || chunkResult.getProcessedRows() < chunkSize) {
                break;
            }

            if (!incremental) {
                break;
            }
        }

        if (processedRows == 0) {
            logger.debug("No results for SQL statement '{}' since ID {}", statementKey, cursor);
            return;
        }

        if (incremental) {
            if (!idObservedAny) {
                logger.warn("SQL statement '{}' is marked incremental but no valid ID value was detected during processing.", statementKey);
            }
            long nextCursor = lastSyncedIds.getOrDefault(statementKey, cursor);
            logger.info("Synced {} row(s) for SQL statement '{}'; next sync cursor ID: {}", processedRows, statementKey, nextCursor);
        } else {
            logger.info("Synced {} row(s) for SQL statement '{}' (non-incremental).", processedRows, statementKey);
        }
    }

    private StatementChunkResult executeStatementChunk(ApplicationProperties.KingBase.SqlStatement statement,
                                                       String statementKey,
                                                       String baseSql,
                                                       long lastId,
                                                       int chunkSize,
                                                       int fetchSize,
                                                       boolean incremental) {
        JdbcTemplate template = kingbaseJdbcTemplate;
        if (template == null) {
            return StatementChunkResult.empty(lastId);
        }

        StatementChunkResult chunkResult = new StatementChunkResult(lastId);
        String sql = resolveSql(baseSql, chunkSize);
        String indexName = resolveIndexName(statement);
        String syncTimestamp = Instant.now().toString();
        String idColumn = statement.getIdColumn();
        String normalizedIdColumn = StringUtils.hasText(idColumn) ? idColumn.toLowerCase(Locale.ROOT) : null;

        try {
            template.query(connection -> {
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                if (fetchSize > 0) {
                    preparedStatement.setFetchSize(fetchSize);
                }
                if (chunkSize > 0) {
                    preparedStatement.setMaxRows(chunkSize);
                }

                int parameterIndex = 1;
                if (incremental) {
                    preparedStatement.setObject(parameterIndex++, lastId);
                }
                return preparedStatement;
            }, resultSet -> {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (resultSet.next()) {
                    Map<String, Object> document = new HashMap<String, Object>(columnCount + 2);
                    Object rawIdValue = null;

                    for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                        String columnLabel = metaData.getColumnLabel(columnIndex);
                        if (!StringUtils.hasText(columnLabel)) {
                            columnLabel = metaData.getColumnName(columnIndex);
                        }
                        Object columnValue = resultSet.getObject(columnIndex);
                        document.put(columnLabel, normalizeValue(columnValue));

                        if (normalizedIdColumn != null && columnLabel != null && columnLabel.toLowerCase(Locale.ROOT).equals(normalizedIdColumn)) {
                            rawIdValue = columnValue;
                        }
                    }

                    document.put("_query", statementKey);
                    document.put("_sync_timestamp", syncTimestamp);

                    String documentId = resolveDocumentId(statement, document, statementKey);
                    elasticsearchService.indexDocument(indexName, documentId, document);

                    chunkResult.incrementProcessedRows();
                    Long rowId = extractId(rawIdValue != null ? rawIdValue : document.get(idColumn));
                    chunkResult.observeId(rowId);
                }
                return null;
            });
        } catch (DataAccessException ex) {
            logger.error("Error executing SQL statement '{}': {}", statement.getName(), ex.getMessage(), ex);
        }

        return chunkResult;
    }

    private ExecutorService createStatementExecutor(int configuredParallelism) {
        final int threads = Math.max(1, configuredParallelism);
        final AtomicInteger index = new AtomicInteger(1);
        ThreadFactory threadFactory = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable);
                thread.setName("kingbase-sql-sync-" + index.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        };
        return Executors.newFixedThreadPool(threads, threadFactory);
    }

    private boolean isExecutable(ApplicationProperties.KingBase.SqlStatement statement) {
        if (statement == null || !statement.isEnabled()) {
            return false;
        }
        if (!StringUtils.hasText(statement.getSql())) {
            logger.warn("Skipping SQL statement '{}' because SQL text is blank.", statement != null ? statement.getName() : "");
            return false;
        }
        return true;
    }

    private int resolveChunkSize(ApplicationProperties.KingBase.SqlStatement statement) {
        Integer chunkSize = statement.getChunkSize();
        if (chunkSize != null && chunkSize.intValue() > 0) {
            return chunkSize.intValue();
        }
        int defaultChunk = kingBaseProperties.getDefaultChunkSize();
        return defaultChunk > 0 ? defaultChunk : 0;
    }

    private int resolveFetchSize(ApplicationProperties.KingBase.SqlStatement statement, int chunkSize) {
        Boolean streamResults = statement.getStreamResults();
        if (streamResults != null && !streamResults.booleanValue()) {
            return 0;
        }

        Integer fetchSize = statement.getFetchSize();
        if (fetchSize != null && fetchSize.intValue() > 0) {
            return fetchSize.intValue();
        }

        if (chunkSize > 0) {
            return chunkSize;
        }

        int defaultFetch = kingBaseProperties.getDefaultFetchSize();
        return defaultFetch > 0 ? defaultFetch : 0;
    }

    private String resolveSql(String sql, int chunkSize) {
        if (!StringUtils.hasText(sql)) {
            return sql;
        }
        if (chunkSize > 0 && sql.contains(":chunkSize")) {
            return sql.replace(":chunkSize", String.valueOf(chunkSize));
        }
        return sql;
    }

    private long resolveLastSyncedId(String statementKey) {
        Long cached = lastSyncedIds.get(statementKey);
        if (cached != null) {
            return cached.longValue();
        }

        KingBaseSyncStateRepository repository = getStateRepository();
        if (repository != null) {
            Optional<Long> persisted = repository.fetchLastId(statementKey);
            if (persisted.isPresent()) {
                long value = persisted.get().longValue();
                lastSyncedIds.put(statementKey, value);
                return value;
            }
        }

        lastSyncedIds.put(statementKey, 0L);
        return 0L;
    }

    private void persistLastSyncedId(String statementKey, long nextId) {
        lastSyncedIds.put(statementKey, nextId);
        KingBaseSyncStateRepository repository = getStateRepository();
        if (repository != null) {
            repository.upsertLastId(statementKey, nextId);
        }
    }

    private KingBaseSyncStateRepository getStateRepository() {
        JdbcTemplate template = kingbaseJdbcTemplate;
        if (template == null) {
            return null;
        }

        KingBaseSyncStateRepository current = stateRepository;
        if (current == null) {
            synchronized (this) {
                current = stateRepository;
                if (current == null) {
                    current = new KingBaseSyncStateRepository(template, kingBaseProperties.getSyncStateTable());
                    current.ensureInitialized();
                    stateRepository = current;
                }
            }
        }
        return current;
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

    private Object normalizeValue(Object value) {
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toInstant().toString();
        }
        if (value instanceof java.sql.Date) {
            return value.toString();
        }
        if (value instanceof java.sql.Time) {
            return value.toString();
        }
        return value;
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

    private static final class StatementChunkResult {
        private int processedRows;
        private boolean idObserved;
        private long maxObservedId;

        private StatementChunkResult(long startingCursor) {
            this.maxObservedId = startingCursor;
        }

        static StatementChunkResult empty(long cursor) {
            return new StatementChunkResult(cursor);
        }

        void incrementProcessedRows() {
            processedRows++;
        }

        void observeId(Long rowId) {
            if (rowId == null) {
                return;
            }
            idObserved = true;
            if (rowId > maxObservedId) {
                maxObservedId = rowId;
            }
        }

        int getProcessedRows() {
            return processedRows;
        }

        boolean isIdObserved() {
            return idObserved;
        }

        long getMaxObservedId() {
            return maxObservedId;
        }
    }
}
