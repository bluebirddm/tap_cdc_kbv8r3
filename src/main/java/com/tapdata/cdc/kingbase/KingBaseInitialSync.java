package com.tapdata.cdc.kingbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Submits an initial KingBase sync once the Spring Boot application is ready.
 * The task is queued onto the same executor used for scheduled runs to keep
 * resource usage consistent.
 */
@Component
@ConditionalOnBean(KingBaseSqlSyncService.class)
class KingBaseInitialSync {

    private static final Logger logger = LoggerFactory.getLogger(KingBaseInitialSync.class);

    private final KingBaseSqlSyncService syncService;

    KingBaseInitialSync(KingBaseSqlSyncService syncService) {
        this.syncService = syncService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void submitInitialSync() {
        logger.info("Submitting initial KingBase SQL sync on application startup.");
        syncService.refreshStatementGroupSchedules();
        syncService.triggerManualSyncAsync();
    }
}
