package com.tapdata.cdc.elasticsearch;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class IndexManager {

    private static final Logger logger = LoggerFactory.getLogger(IndexManager.class);

    @Autowired
    private RestHighLevelClient elasticsearchClient;

    private final Set<String> createdIndices = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    public void ensureIndexExists(String indexName) {
        if (createdIndices.contains(indexName)) {
            return;
        }

        try {
            GetIndexRequest request = new GetIndexRequest(indexName);
            boolean exists = elasticsearchClient.indices().exists(request, RequestOptions.DEFAULT);

            if (!exists) {
                createIndex(indexName);
                logger.info("Created new index: {}", indexName);
            }

            createdIndices.add(indexName);

        } catch (IOException e) {
            logger.error("Error checking/creating index: {}", indexName, e);
            throw new RuntimeException("Failed to ensure index exists: " + indexName, e);
        }
    }

    private void createIndex(String indexName) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);

        Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.max_result_window", 50000)
            .put("analysis.analyzer.ik_analyzer.type", "custom")
            .put("analysis.analyzer.ik_analyzer.tokenizer", "ik_max_word")
            .put("analysis.analyzer.ik_analyzer.filter.0", "lowercase")
            .build();

        request.settings(settings);

        Map<String, Object> mapping = new HashMap<String, Object>();
        mapping.put("properties", createDefaultProperties());
        request.mapping(mapping);

        elasticsearchClient.indices().create(request, RequestOptions.DEFAULT);
    }

    private Map<String, Object> createDefaultProperties() {
        Map<String, Object> properties = new HashMap<String, Object>();

        properties.put("_schema", keywordField());
        properties.put("_table", keywordField());
        properties.put("_sync_timestamp", dateField());

        properties.put("id", longField());

        properties.put("name", textWithKeywordField());
        properties.put("title", textWithKeywordField());
        properties.put("description", textField());
        properties.put("content", textField());
        properties.put("notes", textField());

        properties.put("username", keywordField());
        properties.put("email", keywordField());
        properties.put("full_name", textWithKeywordField());
        properties.put("phone", keywordField());
        properties.put("address", textField());

        properties.put("price", doubleField());
        properties.put("category", keywordField());
        properties.put("stock_quantity", integerField());
        properties.put("sku", keywordField());
        properties.put("is_active", booleanField());

        properties.put("user_id", longField());
        properties.put("product_id", longField());
        properties.put("order_id", longField());
        properties.put("total_amount", doubleField());
        properties.put("status", keywordField());
        properties.put("quantity", integerField());
        properties.put("unit_price", doubleField());
        properties.put("total_price", doubleField());
        properties.put("shipping_address", textField());

        properties.put("created_at", dateField());
        properties.put("updated_at", dateField());
        properties.put("order_date", dateField());

        return properties;
    }

    public void updateMapping(String indexName, Map<String, Object> additionalProperties) {
        if (additionalProperties == null || additionalProperties.isEmpty()) {
            return;
        }
        try {
            PutMappingRequest request = new PutMappingRequest(indexName);
            Map<String, Object> mapping = new HashMap<String, Object>();
            mapping.put("properties", additionalProperties);
            request.source(mapping);
            elasticsearchClient.indices().putMapping(request, RequestOptions.DEFAULT);
            logger.debug("Updated mapping for index: {}", indexName);

        } catch (IOException e) {
            logger.error("Error updating mapping for index: {}", indexName, e);
            throw new RuntimeException("Failed to update mapping for index: " + indexName, e);
        }
    }

    private Map<String, Object> keywordField() {
        Map<String, Object> field = new HashMap<String, Object>();
        field.put("type", "keyword");
        return field;
    }

    private Map<String, Object> textField() {
        Map<String, Object> field = new HashMap<String, Object>();
        field.put("type", "text");
        field.put("analyzer", "ik_analyzer");
        return field;
    }

    private Map<String, Object> textWithKeywordField() {
        Map<String, Object> field = textField();
        Map<String, Object> keyword = keywordField();
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("keyword", keyword);
        field.put("fields", fields);
        return field;
    }

    private Map<String, Object> longField() {
        Map<String, Object> field = new HashMap<String, Object>();
        field.put("type", "long");
        return field;
    }

    private Map<String, Object> integerField() {
        Map<String, Object> field = new HashMap<String, Object>();
        field.put("type", "integer");
        return field;
    }

    private Map<String, Object> doubleField() {
        Map<String, Object> field = new HashMap<String, Object>();
        field.put("type", "double");
        return field;
    }

    private Map<String, Object> booleanField() {
        Map<String, Object> field = new HashMap<String, Object>();
        field.put("type", "boolean");
        return field;
    }

    private Map<String, Object> dateField() {
        Map<String, Object> field = new HashMap<String, Object>();
        field.put("type", "date");
        return field;
    }
}
