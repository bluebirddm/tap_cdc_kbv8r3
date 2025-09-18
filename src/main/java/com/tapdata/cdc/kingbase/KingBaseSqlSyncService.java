package com.tapdata.cdc.kingbase;

import com.tapdata.cdc.config.ApplicationProperties;
import com.tapdata.cdc.elasticsearch.ElasticsearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.time.Instant;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.yaml.snakeyaml.Yaml;

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
    private final ResourceLoader resourceLoader;
    private final Map<String, String> lastSyncedCursors = new ConcurrentHashMap<String, String>();
    private final Map<String, AtomicBoolean> statementLocks = new ConcurrentHashMap<String, AtomicBoolean>();
    private final AtomicBoolean manualTriggerRequested = new AtomicBoolean(false);
    private final ExecutorService statementExecutor;
    private final Map<String, ScheduledFuture<?>> groupSchedules = new ConcurrentHashMap<String, ScheduledFuture<?>>();

    @Autowired(required = false)
    private TaskScheduler taskScheduler;

    private volatile JdbcTemplate kingbaseJdbcTemplate;
    private volatile KingBaseSyncStateRepository stateRepository;

    public KingBaseSqlSyncService(ElasticsearchService elasticsearchService,
                                  ApplicationProperties applicationProperties,
                                  ResourceLoader resourceLoader) {
        this.elasticsearchService = elasticsearchService;
        this.kingBaseProperties = applicationProperties.getKingbase();
        this.resourceLoader = resourceLoader;
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

        List<ApplicationProperties.KingBase.SqlStatement> statements = resolveConfiguredStatements();
        if (CollectionUtils.isEmpty(statements)) {
            logger.info("No KingBase SQL statements configured for sync.");
        } else {
            logger.info("Configured {} KingBase SQL statement(s) for Elasticsearch sync.", statements.size());
        }
    }

    @PreDestroy
    public void shutdownExecutor() {
        cancelGroupSchedules();
        statementExecutor.shutdown();
    }

    @Scheduled(cron = "${tap.kingbase.sql-sync-cron:0 */5 * * * *}")
    public void syncStatements() {
        boolean triggeredManually = manualTriggerRequested.getAndSet(false);

        JdbcTemplate template = kingbaseJdbcTemplate;
        if (template == null) {
            if (triggeredManually) {
                logger.warn("Manual KingBase SQL sync requested but JdbcTemplate is not configured yet.");
            }
            return;
        }

        List<ApplicationProperties.KingBase.SqlStatement> statements = resolveConfiguredStatements();
        if (CollectionUtils.isEmpty(statements)) {
            if (triggeredManually) {
                logger.info("Manual KingBase SQL sync requested but no statements are configured.");
            }
            return;
        }

        dispatchStatements(statements, null, triggeredManually);
    }

    /**
     * Allows callers to request an immediate sync outside the scheduled cadence.
     */
    public void triggerManualSync() {
        manualTriggerRequested.set(true);
        syncStatements();
    }

    /**
     * Enqueues a manual sync onto the statement worker pool so startup hooks can
     * reuse the same threading model without blocking the main thread.
     */
    public void triggerManualSyncAsync() {
        statementExecutor.submit(new Runnable() {
            @Override
            public void run() {
                triggerManualSync();
            }
        });
    }

    private void runSingleStatement(ApplicationProperties.KingBase.SqlStatement statement, String statementKey) {
        String cursor = resolveLastSyncedCursor(statementKey, statement);
        int chunkSize = resolveChunkSize(statement);
        int fetchSize = resolveFetchSize(statement, chunkSize);
        boolean incremental = statement.isIncremental() && StringUtils.hasText(statement.getIdColumn());
        String baseSql = statement.getSql();

        long processedRows = 0;
        String maxObservedCursor = cursor;
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
                    if (compareCursors(chunkResult.getMaxObservedCursor(), maxObservedCursor, statement) > 0) {
                        maxObservedCursor = chunkResult.getMaxObservedCursor();
                        cursor = maxObservedCursor;
                        persistLastSyncedCursor(statementKey, cursor);
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
            logger.debug("No results for SQL statement '{}' since cursor {}", statementKey, cursor);
            return;
        }

        if (incremental) {
            if (!idObservedAny) {
                logger.warn("SQL statement '{}' is marked incremental but no valid ID value was detected during processing.", statementKey);
            }
            String nextCursor = lastSyncedCursors.getOrDefault(statementKey, cursor);
            logger.info("Synced {} row(s) for SQL statement '{}'; next sync cursor: {}", processedRows, statementKey, nextCursor);
        } else {
            logger.info("Synced {} row(s) for SQL statement '{}' (non-incremental).", processedRows, statementKey);
        }
    }

    private boolean acquireStatementLock(String statementKey) {
        AtomicBoolean lock = statementLocks.computeIfAbsent(statementKey, key -> new AtomicBoolean(false));
        return lock.compareAndSet(false, true);
    }

    private void releaseStatementLock(String statementKey) {
        AtomicBoolean lock = statementLocks.get(statementKey);
        if (lock != null) {
            lock.set(false);
        }
    }

    public void refreshStatementGroupSchedules() {
        cancelGroupSchedules();
        scheduleStatementGroups();
    }

    public int clearSyncState() {
        KingBaseSyncStateRepository repository = getStateRepository();
        if (repository == null) {
            logger.warn("Cannot clear KingBase sync state because JdbcTemplate is not configured.");
            return 0;
        }
        int deleted = repository.deleteAll();
        lastSyncedCursors.clear();
        if (deleted > 0) {
            logger.info("Cleared {} entry(ies) from KingBase sync state table '{}'.", deleted, kingBaseProperties.getSyncStateTable());
        } else {
            logger.info("KingBase sync state table '{}' was already empty.", kingBaseProperties.getSyncStateTable());
        }
        return deleted;
    }

    private StatementChunkResult executeStatementChunk(ApplicationProperties.KingBase.SqlStatement statement,
                                                       String statementKey,
                                                       String baseSql,
                                                       String lastCursor,
                                                       int chunkSize,
                                                       int fetchSize,
                                                       boolean incremental) {
        JdbcTemplate template = kingbaseJdbcTemplate;
        if (template == null) {
            return StatementChunkResult.empty(lastCursor);
        }

        StatementChunkResult chunkResult = new StatementChunkResult(lastCursor);
        String sql = resolveSql(baseSql, chunkSize);
        final boolean requiresParam = expectsCursorParam(sql) || incremental;
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
                if (requiresParam) {
                    Object param = convertCursorParam(lastCursor, statement);
                    preparedStatement.setObject(parameterIndex++, param);
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
                    Object raw = rawIdValue != null ? rawIdValue : document.get(idColumn);
                    chunkResult.observeCursor(asCursorValue(raw));
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

    private List<ApplicationProperties.KingBase.SqlStatement> resolveConfiguredStatements() {
        List<ApplicationProperties.KingBase.SqlStatement> combined = new ArrayList<ApplicationProperties.KingBase.SqlStatement>();
        List<ApplicationProperties.KingBase.SqlStatement> inlineStatements = kingBaseProperties.getSqlStatements();
        if (!CollectionUtils.isEmpty(inlineStatements)) {
            combined.addAll(inlineStatements);
        }
        combined.addAll(loadStatementsFromFile(kingBaseProperties.getSqlStatementsFile()));
        return combined;
    }

    private List<ApplicationProperties.KingBase.SqlStatement> loadStatementsFromFile(String location) {
        if (!StringUtils.hasText(location)) {
            return Collections.<ApplicationProperties.KingBase.SqlStatement>emptyList();
        }

        Resource resource = resolveStatementsResource(location);
        if (resource == null || !resource.exists() || !resource.isReadable()) {
            logger.warn("Configured SQL statements file '{}' could not be found or is not readable.", location);
            return Collections.<ApplicationProperties.KingBase.SqlStatement>emptyList();
        }

        try (InputStream inputStream = resource.getInputStream()) {
            Yaml yaml = new Yaml();
            Object loaded = yaml.load(inputStream);
            if (!(loaded instanceof Map)) {
                logger.warn("SQL statements file '{}' did not contain a YAML object.", resource.getDescription());
                return Collections.<ApplicationProperties.KingBase.SqlStatement>emptyList();
            }

            Map<?, ?> root = (Map<?, ?>) loaded;
            List<ApplicationProperties.KingBase.SqlStatement> results = new ArrayList<ApplicationProperties.KingBase.SqlStatement>(root.size());
            for (Map.Entry<?, ?> entry : root.entrySet()) {
                if (!(entry.getValue() instanceof Map)) {
                    logger.warn("Skipping SQL statement '{}' because its configuration is not a map.", entry.getKey());
                    continue;
                }

                ApplicationProperties.KingBase.SqlStatement statement = new ApplicationProperties.KingBase.SqlStatement();
                if (entry.getKey() != null) {
                    statement.setName(entry.getKey().toString());
                }
                applyFileStatementProperties(statement, (Map<?, ?>) entry.getValue());
                results.add(statement);
            }
            return results;
        } catch (Exception ex) {
            logger.error("Failed to load KingBase SQL statements from '{}': {}", resource.getDescription(), ex.getMessage(), ex);
            return Collections.<ApplicationProperties.KingBase.SqlStatement>emptyList();
        }
    }

    private Resource resolveStatementsResource(String location) {
        String normalizedLocation = location.trim();

        // Prefer a file system resource located relative to the working directory or the provided path.
        Resource fileResource = resolveFileSystemResource(normalizedLocation);
        if (fileResource != null && fileResource.exists() && fileResource.isReadable()) {
            return fileResource;
        }

        // Fall back to the original resource location (classpath:, file:, etc.).
        Resource resource = resourceLoader.getResource(normalizedLocation);
        if (normalizedLocation.startsWith("classpath:")) {
            // Allow overriding classpath resources with a file of the same name placed next to the JAR.
            String fallbackName = normalizedLocation.substring("classpath:".length());
            Resource override = resolveFileSystemResource(fallbackName);
            if (override != null && override.exists() && override.isReadable()) {
                return override;
            }
        }
        return resource;
    }

    private Resource resolveFileSystemResource(String candidate) {
        if (!StringUtils.hasText(candidate) || candidate.startsWith("classpath:")) {
            return null;
        }

        String withoutPrefix = candidate;
        if (candidate.startsWith("file:")) {
            withoutPrefix = candidate.substring("file:".length());
        }

        Path path = Paths.get(withoutPrefix);
        if (!path.isAbsolute()) {
            path = Paths.get(System.getProperty("user.dir"), withoutPrefix);
        }

        try {
            Path normalized = path.toAbsolutePath().normalize();
            if (!Files.exists(normalized)) {
                return null;
            }
            return resourceLoader.getResource("file:" + normalized.toString());
        } catch (Exception ex) {
            logger.debug("Unable to resolve file system resource for '{}': {}", candidate, ex.getMessage());
            return null;
        }
    }

    private void applyFileStatementProperties(ApplicationProperties.KingBase.SqlStatement statement, Map<?, ?> properties) {
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            String key = entry.getKey().toString().trim().toLowerCase(Locale.ROOT).replace("-", "").replace("_", "");
            Object value = entry.getValue();

            if ("sql".equals(key)) {
                statement.setSql(asString(value));
            } else if ("index".equals(key)) {
                statement.setIndex(asString(value));
            } else if ("idcolumn".equals(key)) {
                statement.setIdColumn(asString(value));
            } else if ("idtype".equals(key)) {
                statement.setIdType(asString(value));
            } else if ("incremental".equals(key)) {
                Boolean bool = asBoolean(value);
                if (bool != null) {
                    statement.setIncremental(bool.booleanValue());
                }
            } else if ("enabled".equals(key)) {
                Boolean bool = asBoolean(value);
                if (bool != null) {
                    statement.setEnabled(bool.booleanValue());
                }
            } else if ("chunksize".equals(key)) {
                Integer intValue = asInteger(value);
                statement.setChunkSize(intValue);
            } else if ("fetchsize".equals(key)) {
                Integer intValue = asInteger(value);
                statement.setFetchSize(intValue);
            } else if ("streamresults".equals(key)) {
                Boolean bool = asBoolean(value);
                if (bool != null) {
                    statement.setStreamResults(bool);
                }
            }
        }
    }

    private String asString(Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    private Integer asInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return Integer.valueOf(((Number) value).intValue());
        }
        if (value instanceof CharSequence) {
            try {
                return Integer.valueOf(Integer.parseInt(value.toString()));
            } catch (NumberFormatException ex) {
                logger.warn("Unable to parse integer value '{}'.", value);
            }
        }
        return null;
    }

    private Boolean asBoolean(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        if (value instanceof CharSequence) {
            String text = value.toString().trim().toLowerCase(Locale.ROOT);
            if ("true".equals(text) || "yes".equals(text) || "on".equals(text)) {
                return Boolean.TRUE;
            }
            if ("false".equals(text) || "no".equals(text) || "off".equals(text)) {
                return Boolean.FALSE;
            }
        }
        return null;
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

    private String resolveLastSyncedCursor(String statementKey, ApplicationProperties.KingBase.SqlStatement statement) {
        String cached = lastSyncedCursors.get(statementKey);
        if (cached != null) {
            return cached;
        }

        KingBaseSyncStateRepository repository = getStateRepository();
        if (repository != null) {
            Optional<String> persisted = repository.fetchLastCursor(statementKey);
            if (persisted.isPresent() && StringUtils.hasText(persisted.get())) {
                String value = persisted.get();
                lastSyncedCursors.put(statementKey, value);
                return value;
            }
            // Backward compatibility: read numeric last_id
            Optional<Long> numeric = repository.fetchLastId(statementKey);
            if (numeric.isPresent()) {
                String value = String.valueOf(numeric.get().longValue());
                lastSyncedCursors.put(statementKey, value);
                return value;
            }
        }

        String initial = isNumericId(statement) ? "0" : "";
        lastSyncedCursors.put(statementKey, initial);
        return initial;
    }

    private void persistLastSyncedCursor(String statementKey, String nextCursor) {
        lastSyncedCursors.put(statementKey, nextCursor);
        KingBaseSyncStateRepository repository = getStateRepository();
        if (repository != null) {
            repository.upsertLastCursor(statementKey, nextCursor);
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

    private String asCursorValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toInstant().toString();
        }
        return String.valueOf(value);
    }

    private void cancelGroupSchedules() {
        for (ScheduledFuture<?> future : groupSchedules.values()) {
            if (future != null) {
                future.cancel(true);
            }
        }
        groupSchedules.clear();
    }

    private void scheduleStatementGroups() {
        List<ApplicationProperties.KingBase.StatementGroup> groups = kingBaseProperties.getSqlStatementGroups();
        if (CollectionUtils.isEmpty(groups)) {
            return;
        }

        if (taskScheduler == null) {
            logger.warn("SQL statement groups are configured but no TaskScheduler bean is available; group scheduling is disabled.");
            return;
        }

        for (ApplicationProperties.KingBase.StatementGroup group : groups) {
            if (group == null || !StringUtils.hasText(group.getFile())) {
                logger.warn("Skipping SQL statement group because file location is blank.");
                continue;
            }

            final ApplicationProperties.KingBase.StatementGroup currentGroup = group;
            String groupKey = resolveGroupKey(currentGroup);
            String cron = StringUtils.hasText(currentGroup.getCron()) ? currentGroup.getCron() : kingBaseProperties.getSqlSyncCron();

            Runnable scheduledTask = new Runnable() {
                @Override
                public void run() {
                    syncStatementsForGroup(currentGroup);
                }
            };

            ScheduledFuture<?> future = taskScheduler.schedule(scheduledTask, new CronTrigger(cron));
            ScheduledFuture<?> previous = groupSchedules.put(groupKey, future);
            if (previous != null) {
                previous.cancel(true);
            }
            logger.info("Scheduled KingBase SQL statement group '{}' with cron '{}'.", groupKey, cron);

            try {
                taskScheduler.schedule(new Runnable() {
                    @Override
                    public void run() {
                        syncStatementsForGroup(currentGroup);
                    }
                }, java.util.Date.from(java.time.Instant.now()));
            } catch (Exception ex) {
                logger.warn("Failed to execute initial sync for group '{}': {}", groupKey, ex.getMessage());
            }
        }
    }

    private void syncStatementsForGroup(ApplicationProperties.KingBase.StatementGroup group) {
        List<ApplicationProperties.KingBase.SqlStatement> statements = loadStatementsFromFile(group.getFile());
        if (CollectionUtils.isEmpty(statements)) {
            logger.debug("No statements loaded for group '{}'.", resolveGroupKey(group));
            return;
        }

        String groupPrefix = sanitizeGroupPrefix(resolveGroupKey(group));
        dispatchStatements(statements, groupPrefix, false);
    }

    private void dispatchStatements(List<ApplicationProperties.KingBase.SqlStatement> statements,
                                    String groupPrefix,
                                    boolean triggeredManually) {
        getStateRepository();

        int dispatchedStatements = 0;

        for (ApplicationProperties.KingBase.SqlStatement statement : statements) {
            if (!isExecutable(statement)) {
                continue;
            }

            final ApplicationProperties.KingBase.SqlStatement currentStatement = statement;
            final String statementKey = composeStatementKey(currentStatement, groupPrefix);
            if (!acquireStatementLock(statementKey)) {
                logger.debug("Skipping SQL statement '{}' because a previous execution is still running.", statementKey);
                continue;
            }

            statementExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        runSingleStatement(currentStatement, statementKey);
                    } catch (Exception ex) {
                        logger.error("Failed to sync SQL statement '{}': {}", currentStatement.getName(), ex.getMessage(), ex);
                    } finally {
                        releaseStatementLock(statementKey);
                    }
                }
            });
            dispatchedStatements++;
        }

        if (triggeredManually && dispatchedStatements > 0) {
            logger.info("Manual KingBase SQL sync dispatched {} statement task(s).", dispatchedStatements);
        }
    }

    private String composeStatementKey(ApplicationProperties.KingBase.SqlStatement statement, String groupPrefix) {
        String baseKey = resolveStatementKey(statement);
        if (!StringUtils.hasText(groupPrefix)) {
            return baseKey;
        }
        return groupPrefix + "__" + baseKey;
    }

    private boolean expectsCursorParam(String sql) {
        if (!StringUtils.hasText(sql)) {
            return false;
        }
        // Simple heuristic: a single positional parameter is expected when a '?' exists.
        // (We intentionally avoid heavy SQL parsing.)
        return sql.indexOf('?') >= 0;
    }

    private boolean isNumericId(ApplicationProperties.KingBase.SqlStatement statement) {
        String type = statement.getIdType();
        if (!StringUtils.hasText(type)) {
            return true;
        }
        String normalized = type.trim().toLowerCase(Locale.ROOT);
        return normalized.startsWith("num");
    }

    private Object convertCursorParam(String cursor, ApplicationProperties.KingBase.SqlStatement statement) {
        if (cursor == null || cursor.isEmpty()) {
            return isNumericId(statement) ? 0L : " ";
        }
        if (isNumericId(statement)) {
            try {
                return Long.parseLong(cursor);
            } catch (NumberFormatException ex) {
                return 0L;
            }
        }
        return cursor;
    }

    private int compareCursors(String a, String b, ApplicationProperties.KingBase.SqlStatement statement) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        if (isNumericId(statement)) {
            try {
                long la = Long.parseLong(a);
                long lb = Long.parseLong(b);
                return Long.compare(la, lb);
            } catch (NumberFormatException ex) {
                // fallback to lexicographic
            }
        }
        return a.compareTo(b);
    }

    private String resolveGroupKey(ApplicationProperties.KingBase.StatementGroup group) {
        if (group == null) {
            return "default";
        }
        if (StringUtils.hasText(group.getName())) {
            return group.getName();
        }
        if (StringUtils.hasText(group.getFile())) {
            return group.getFile();
        }
        return "group_" + Integer.toHexString(System.identityHashCode(group));
    }

    private String sanitizeGroupPrefix(String groupPrefix) {
        if (!StringUtils.hasText(groupPrefix)) {
            return groupPrefix;
        }
        return groupPrefix.replaceAll("[^a-zA-Z0-9_-]", "_");
    }

    private static final class StatementChunkResult {
        private int processedRows;
        private boolean idObserved;
        private String maxObservedCursor;

        private StatementChunkResult(String startingCursor) {
            this.maxObservedCursor = startingCursor;
        }

        static StatementChunkResult empty(String cursor) {
            return new StatementChunkResult(cursor);
        }

        void incrementProcessedRows() {
            processedRows++;
        }

        void observeCursor(String cursor) {
            if (cursor == null) {
                return;
            }
            idObserved = true;
            if (maxObservedCursor == null || cursor.compareTo(maxObservedCursor) > 0) {
                maxObservedCursor = cursor;
            }
        }

        int getProcessedRows() {
            return processedRows;
        }

        boolean isIdObserved() {
            return idObserved;
        }

        String getMaxObservedCursor() {
            return maxObservedCursor;
        }
    }
}
