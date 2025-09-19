package com.tapdata.cdc.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SearchService {

    private static final Logger logger = LoggerFactory.getLogger(SearchService.class);
    private static final int MAX_PAGE_SIZE = 100;

    @Autowired
    private RestHighLevelClient elasticsearchClient;

    public SearchResult searchAllTables(String keyword, int page, int size) {
        return searchAllTables(keyword, null, null, page, size);
    }

    public SearchResult searchAllTables(String keyword, String schema, String table, int page, int size) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = buildSearchQuery(keyword, schema, table);

        sourceBuilder.query(boolQuery);
        configurePaging(sourceBuilder, page, size);
        configureSorting(sourceBuilder);
        configureHighlight(sourceBuilder);

        return executeSearch(new String[] {"*"}, sourceBuilder);
    }

    public SearchResult searchAllTablesWithoutSource(String keyword, int page, int size) {
        return searchAllTablesWithoutSource(keyword, null, null, page, size);
    }

    public SearchResult searchAllTablesWithoutSource(String keyword, String schema, String table, int page, int size) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = buildSearchQuery(keyword, schema, table);

        sourceBuilder.query(boolQuery);
        configurePaging(sourceBuilder, page, size);
        configureSorting(sourceBuilder);
        sourceBuilder.fetchSource(false);

        return executeSearch(new String[] {"*"}, sourceBuilder);
    }

    public SearchResult searchByTable(String schema, String table, String keyword, int page, int size) {
        String indexName = generateIndexName(schema, table);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        if (hasText(keyword)) {
            boolQuery.must(QueryBuilders.multiMatchQuery(keyword)
                .field("*")
                .operator(Operator.OR)
                .fuzziness(Fuzziness.AUTO));
        } else {
            boolQuery.must(QueryBuilders.matchAllQuery());
        }

        sourceBuilder.query(boolQuery);
        configurePaging(sourceBuilder, page, size);
        configureSorting(sourceBuilder);
        configureHighlight(sourceBuilder);

        return executeSearch(new String[] {indexName}, sourceBuilder);
    }

    public List<String> getAvailableIndices() {
        try {
            GetIndexRequest request = new GetIndexRequest("*");
            GetIndexResponse response = elasticsearchClient.indices().get(request, RequestOptions.DEFAULT);
            return Arrays.asList(response.getIndices());
        } catch (IOException e) {
            logger.error("Error getting available indices", e);
            return Collections.emptyList();
        }
    }

    private SearchResult executeSearch(String[] indices, SearchSourceBuilder sourceBuilder) {
        try {
            SearchRequest searchRequest = new SearchRequest(indices);
            searchRequest.source(sourceBuilder);

            long startTime = System.currentTimeMillis();
            SearchResponse response = elasticsearchClient.search(searchRequest, RequestOptions.DEFAULT);
            long duration = System.currentTimeMillis() - startTime;

            return buildSearchResult(response, duration);
        } catch (IOException e) {
            logger.error("Error executing search request", e);
            throw new RuntimeException("Search execution failed", e);
        }
    }

    private SearchResult buildSearchResult(SearchResponse response, long duration) {
        SearchResult result = new SearchResult();
        result.setDuration(duration);
        result.setTotalHits(response.getHits().getTotalHits() != null ? response.getHits().getTotalHits().value : 0);

        Float maxScore = response.getHits().getMaxScore();
        if (maxScore != null && !Float.isNaN(maxScore)) {
            result.setMaxScore(Double.valueOf(maxScore));
        }

        List<SearchResult.SearchHit> hits = new ArrayList<SearchResult.SearchHit>();
        for (SearchHit hit : response.getHits().getHits()) {
            SearchResult.SearchHit searchHit = new SearchResult.SearchHit();
            searchHit.setIndex(hit.getIndex());
            searchHit.setId(hit.getId());

            float score = hit.getScore();
            if (!Float.isNaN(score)) {
                searchHit.setScore(Double.valueOf(score));
            }
            searchHit.setSource(hit.getSourceAsMap());

            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (highlightFields != null && !highlightFields.isEmpty()) {
                Map<String, List<String>> highlights = new HashMap<String, List<String>>();
                for (Map.Entry<String, HighlightField> entry : highlightFields.entrySet()) {
                    HighlightField field = entry.getValue();
                    List<String> fragments = new ArrayList<String>();
                    if (field != null && field.fragments() != null) {
                        Text[] texts = field.fragments();
                        for (int i = 0; i < texts.length; i++) {
                            fragments.add(texts[i].string());
                        }
                    }
                    highlights.put(entry.getKey(), fragments);
                }
                searchHit.setHighlight(highlights);
            }

            hits.add(searchHit);
        }

        result.setHits(hits);
        return result;
    }

    private BoolQueryBuilder buildSearchQuery(String keyword, String schema, String table) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        boolean hasQuery = false;
        if (hasText(keyword)) {
            boolQuery.must(QueryBuilders.multiMatchQuery(keyword)
                .field("*")
                .operator(Operator.OR)
                .fuzziness(Fuzziness.AUTO));
            hasQuery = true;
        }

        if (hasText(schema)) {
            boolQuery.filter(QueryBuilders.termQuery("_schema", schema));
            hasQuery = true;
        }

        if (hasText(table)) {
            boolQuery.filter(QueryBuilders.termQuery("_table", table));
            hasQuery = true;
        }

        if (!hasQuery) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        }
        return boolQuery;
    }

    private void configurePaging(SearchSourceBuilder sourceBuilder, int page, int size) {
        int safePage = page < 0 ? 0 : page;
        int safeSize = size <= 0 ? 20 : Math.min(size, MAX_PAGE_SIZE);
        sourceBuilder.from(safePage * safeSize);
        sourceBuilder.size(safeSize);
        sourceBuilder.trackTotalHits(true);
    }

    private void configureSorting(SearchSourceBuilder sourceBuilder) {
        sourceBuilder.sort(SortBuilders.scoreSort());
        sourceBuilder.sort(SortBuilders.fieldSort("_sync_timestamp").order(SortOrder.DESC));
    }

    private void configureHighlight(SearchSourceBuilder sourceBuilder) {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.requireFieldMatch(false);
        HighlightBuilder.Field field = new HighlightBuilder.Field("*")
            .preTags("<mark>")
            .postTags("</mark>")
            .fragmentSize(150)
            .numOfFragments(3);
        highlightBuilder.field(field);
        sourceBuilder.highlighter(highlightBuilder);
    }

    private String generateIndexName(String schema, String table) {
        String raw = (schema == null ? "" : schema) + "_" + (table == null ? "" : table);
        String indexName = raw.toLowerCase().replaceAll("[^a-z0-9_-]", "_");
        if (indexName.startsWith("_")) {
            indexName = "idx" + indexName;
        }
        return indexName;
    }

    private boolean hasText(String value) {
        return value != null && value.trim().length() > 0;
    }
}
