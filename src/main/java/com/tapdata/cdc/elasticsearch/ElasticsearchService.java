package com.tapdata.cdc.elasticsearch;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class ElasticsearchService {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchService.class);

    private static final int BULK_SIZE = 100;
    private static final int BULK_FLUSH_INTERVAL_SECONDS = 5;

    @Autowired
    private RestHighLevelClient elasticsearchClient;

    @Autowired
    private IndexManager indexManager;

    private final BlockingQueue<BulkWriteOperation> bulkQueue = new LinkedBlockingQueue<BulkWriteOperation>();
    private final ScheduledExecutorService bulkProcessor = new ScheduledThreadPoolExecutor(1);
    private final AtomicLong queuedLogCounter = new AtomicLong(0L);
    private static final long LOG_EVERY = Long.getLong("tap.es.logEvery", 1000L);

    public ElasticsearchService() {
        startBulkProcessor();
    }

    public void indexDocument(String indexName, String documentId, Map<String, Object> document) {
        try {
            indexManager.ensureIndexExists(indexName);
            bulkQueue.offer(BulkWriteOperation.index(indexName, documentId, new HashMap<String, Object>(document)));
            maybeLogQueued("index", indexName, documentId);
        } catch (Exception e) {
            logger.error("Error indexing document: index={}, id={}", indexName, documentId, e);
            indexDocumentDirectly(indexName, documentId, document);
        }
    }

    public void updateDocument(String indexName, String documentId, Map<String, Object> document) {
        try {
            indexManager.ensureIndexExists(indexName);
            bulkQueue.offer(BulkWriteOperation.update(indexName, documentId, new HashMap<String, Object>(document)));
            maybeLogQueued("update", indexName, documentId);
        } catch (Exception e) {
            logger.error("Error updating document: index={}, id={}", indexName, documentId, e);
            updateDocumentDirectly(indexName, documentId, document);
        }
    }

    public void deleteDocument(String indexName, String documentId) {
        try {
            bulkQueue.offer(BulkWriteOperation.delete(indexName, documentId));
            maybeLogQueued("delete", indexName, documentId);
        } catch (Exception e) {
            logger.error("Error deleting document: index={}, id={}", indexName, documentId, e);
            deleteDocumentDirectly(indexName, documentId);
        }
    }

    private void maybeLogQueued(String op, String indexName, String documentId) {
        if (!logger.isDebugEnabled()) {
            return;
        }
        long count = queuedLogCounter.incrementAndGet();
        if (LOG_EVERY <= 1L || (count % LOG_EVERY == 0L)) {
            logger.debug("Queued {} operations so far; last op={}, index={}, id={}", count, op, indexName, documentId);
        }
    }

    private void startBulkProcessor() {
        bulkProcessor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                processBulkQueue();
            }
        },
            BULK_FLUSH_INTERVAL_SECONDS,
            BULK_FLUSH_INTERVAL_SECONDS,
            TimeUnit.SECONDS
        );

        Thread queueMonitor = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        if (bulkQueue.size() >= BULK_SIZE) {
                            processBulkQueue();
                        }
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        });
        queueMonitor.setDaemon(true);
        queueMonitor.start();
    }

    private void processBulkQueue() {
        if (bulkQueue.isEmpty()) {
            return;
        }

        List<BulkWriteOperation> operations = new ArrayList<BulkWriteOperation>();
        bulkQueue.drainTo(operations, BULK_SIZE);

        if (operations.isEmpty()) {
            return;
        }

        BulkRequest bulkRequest = new BulkRequest();
        for (BulkWriteOperation operation : operations) {
            switch (operation.type) {
                case INDEX:
                    bulkRequest.add(new IndexRequest(operation.indexName)
                        .id(operation.documentId)
                        .source(operation.document));
                    break;
                case UPDATE:
                    bulkRequest.add(new UpdateRequest(operation.indexName, operation.documentId)
                        .doc(operation.document)
                        .docAsUpsert(true));
                    break;
                case DELETE:
                    bulkRequest.add(new DeleteRequest(operation.indexName, operation.documentId));
                    break;
                default:
                    break;
            }
        }

        if (bulkRequest.numberOfActions() == 0) {
            return;
        }

        try {
            BulkResponse response = elasticsearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);

            if (response.hasFailures()) {
                for (BulkItemResponse item : response.getItems()) {
                    if (item.isFailed()) {
                        logger.error("Bulk operation error: index={}, id={}, error={}",
                            item.getIndex(), item.getId(), item.getFailureMessage());
                    }
                }
            } else {
                logger.debug("Bulk operation completed successfully: {} operations", operations.size());
            }

        } catch (IOException e) {
            logger.error("Error executing bulk operations", e);
        }
    }

    /**
     * Forces an immediate processing of the queued bulk operations.
     */
    public void flushPendingBulkOperations() {
        processBulkQueue();
    }

    private void indexDocumentDirectly(String indexName, String documentId, Map<String, Object> document) {
        try {
            indexManager.ensureIndexExists(indexName);
            IndexRequest request = new IndexRequest(indexName)
                .id(documentId)
                .source(document);

            IndexResponse response = elasticsearchClient.index(request, RequestOptions.DEFAULT);
            logger.debug("Direct index operation: index={}, id={}, result={}",
                indexName, documentId, response.getResult());

        } catch (IOException e) {
            logger.error("Error in direct index operation: index={}, id={}", indexName, documentId, e);
        }
    }

    private void updateDocumentDirectly(String indexName, String documentId, Map<String, Object> document) {
        try {
            indexManager.ensureIndexExists(indexName);
            UpdateRequest request = new UpdateRequest(indexName, documentId)
                .doc(document)
                .docAsUpsert(true);

            UpdateResponse response = elasticsearchClient.update(request, RequestOptions.DEFAULT);
            logger.debug("Direct update operation: index={}, id={}, result={}",
                indexName, documentId, response.getResult());

        } catch (IOException e) {
            logger.error("Error in direct update operation: index={}, id={}", indexName, documentId, e);
        }
    }

    private void deleteDocumentDirectly(String indexName, String documentId) {
        try {
            DeleteRequest request = new DeleteRequest(indexName, documentId);
            DeleteResponse response = elasticsearchClient.delete(request, RequestOptions.DEFAULT);
            logger.debug("Direct delete operation: index={}, id={}, result={}",
                indexName, documentId, response.getResult());

        } catch (IOException e) {
            logger.error("Error in direct delete operation: index={}, id={}", indexName, documentId, e);
        }
    }

    public void shutdown() {
        logger.info("Shutting down Elasticsearch service...");

        processBulkQueue();

        bulkProcessor.shutdown();

        try {
            if (!bulkProcessor.awaitTermination(30, TimeUnit.SECONDS)) {
                bulkProcessor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            bulkProcessor.shutdownNow();
        }

        logger.info("Elasticsearch service shutdown completed");
    }

    private static class BulkWriteOperation {
        private enum Type {
            INDEX,
            UPDATE,
            DELETE
        }

        private final Type type;
        private final String indexName;
        private final String documentId;
        private final Map<String, Object> document;

        private BulkWriteOperation(Type type, String indexName, String documentId, Map<String, Object> document) {
            this.type = type;
            this.indexName = indexName;
            this.documentId = documentId;
            this.document = document;
        }

        private static BulkWriteOperation index(String indexName, String documentId, Map<String, Object> document) {
            return new BulkWriteOperation(Type.INDEX, indexName, documentId, document);
        }

        private static BulkWriteOperation update(String indexName, String documentId, Map<String, Object> document) {
            return new BulkWriteOperation(Type.UPDATE, indexName, documentId, document);
        }

        private static BulkWriteOperation delete(String indexName, String documentId) {
            return new BulkWriteOperation(Type.DELETE, indexName, documentId, null);
        }
    }
}
