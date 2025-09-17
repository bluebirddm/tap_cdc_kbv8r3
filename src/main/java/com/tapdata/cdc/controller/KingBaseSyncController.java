package com.tapdata.cdc.controller;

import com.tapdata.cdc.kingbase.KingBaseSqlSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Provides an endpoint to trigger the KingBase SQL sync manually.
 */
@RestController
@RequestMapping("/kingbase")
public class KingBaseSyncController {

    private static final Logger logger = LoggerFactory.getLogger(KingBaseSyncController.class);

    private final KingBaseSqlSyncService syncService;

    public KingBaseSyncController(KingBaseSqlSyncService syncService) {
        this.syncService = syncService;
    }

    @PostMapping("/sync")
    public ResponseEntity<Void> triggerManualSync() {
        logger.info("Received request to trigger KingBase manual sync.");
        syncService.triggerManualSync();
        return ResponseEntity.accepted().build();
    }
}
