package com.tapdata.cdc.kingbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;

/**
 * Persists the last processed cursor for each SQL statement to ensure sync resumes
 * after application restarts.
 */
class KingBaseSyncStateRepository {

    private static final Logger logger = LoggerFactory.getLogger(KingBaseSyncStateRepository.class);

    private final JdbcTemplate jdbcTemplate;
    private final String tableName;
    private volatile boolean initialized;

    KingBaseSyncStateRepository(JdbcTemplate jdbcTemplate, String tableName) {
        this.jdbcTemplate = jdbcTemplate;
        this.tableName = tableName;
    }

    void ensureInitialized() {
        if (initialized) {
            return;
        }
        synchronized (this) {
            if (initialized) {
                return;
            }
            try {
                jdbcTemplate.execute(String.format(
                    "CREATE TABLE IF NOT EXISTS %s (" +
                        "statement_name VARCHAR(255) PRIMARY KEY, " +
                        "last_id BIGINT NOT NULL, " +
                        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                    ")",
                    tableName
                ));
                initialized = true;
            } catch (Exception ex) {
                logger.warn("Unable to ensure sync state table '{}': {}", tableName, ex.getMessage());
            }
        }
    }

    Optional<Long> fetchLastId(String statementName) {
        ensureInitialized();
        try {
            Long value = jdbcTemplate.queryForObject(
                String.format("SELECT last_id FROM %s WHERE statement_name = ?", tableName),
                Long.class,
                statementName
            );
            return Optional.ofNullable(value);
        } catch (EmptyResultDataAccessException ignored) {
            return Optional.empty();
        } catch (Exception ex) {
            logger.warn("Failed to fetch sync state for '{}': {}", statementName, ex.getMessage());
            return Optional.empty();
        }
    }

    void upsertLastId(String statementName, long lastId) {
        ensureInitialized();
        try {
            jdbcTemplate.update(
                String.format(
                    "INSERT INTO %s (statement_name, last_id, updated_at) " +
                        "VALUES (?, ?, CURRENT_TIMESTAMP) " +
                        "ON CONFLICT (statement_name) DO UPDATE SET last_id = EXCLUDED.last_id, updated_at = CURRENT_TIMESTAMP",
                    tableName
                ),
                statementName,
                lastId
            );
        } catch (Exception ex) {
            logger.warn("Failed to persist sync state for '{}': {}", statementName, ex.getMessage());
        }
    }
}
