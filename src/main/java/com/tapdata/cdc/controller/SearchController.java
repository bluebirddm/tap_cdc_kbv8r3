package com.tapdata.cdc.controller;

import com.tapdata.cdc.kingbase.KingBaseSqlSyncService;
import com.tapdata.cdc.search.SearchResult;
import com.tapdata.cdc.search.SearchService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@RestController
@RequestMapping("/api/search")
@CrossOrigin(origins = "*")
public class SearchController {

    private final SearchService searchService;
    private final KingBaseSqlSyncService kingBaseSqlSyncService;

    public SearchController(SearchService searchService,
                            ObjectProvider<KingBaseSqlSyncService> kingBaseSqlSyncServiceProvider) {
        this.searchService = searchService;
        this.kingBaseSqlSyncService = kingBaseSqlSyncServiceProvider.getIfAvailable();
    }

    /**
     * 全局搜索 - 在所有表中搜索关键字
     *
     * @param keyword 搜索关键字
     * @param page    页码（从0开始）
     * @param size    每页大小
     * @return 搜索结果
     */
    @GetMapping("/global")
    public ResponseEntity<SearchResult> globalSearch(
            @RequestParam(required = false) String keyword,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        if (page < 0) page = 0;
        if (size <= 0 || size > 100) size = 20;

        SearchResult result = searchService.searchAllTables(keyword, page, size);
        return ResponseEntity.ok(result);
    }

    /**
     * 全局搜索（仅返回索引与去前缀后的文档 ID）
     */
    @GetMapping("/global/ids")
    public ResponseEntity<SearchResult> globalSearchIds(
            @RequestParam(required = false) String keyword,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        if (page < 0) page = 0;
        if (size <= 0 || size > 100) size = 20;

        SearchResult result = searchService.searchAllTablesWithoutSource(keyword, page, size);

        SearchResult trimmed = new SearchResult();
        trimmed.setTotalHits(result.getTotalHits());
        trimmed.setMaxScore(result.getMaxScore());
        trimmed.setDuration(result.getDuration());
        trimmed.setHits(compactHits(result.getHits()));

        return ResponseEntity.ok(trimmed);
    }

    /**
     * 按schema和table搜索
     *
     * @param schema  数据库schema
     * @param table   表名
     * @param keyword 搜索关键字
     * @param page    页码（从0开始）
     * @param size    每页大小
     * @return 搜索结果
     */
    @GetMapping("/table")
    public ResponseEntity<SearchResult> searchByTable(
            @RequestParam String schema,
            @RequestParam String table,
            @RequestParam(required = false) String keyword,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        if (page < 0) page = 0;
        if (size <= 0 || size > 100) size = 20;

        SearchResult result = searchService.searchByTable(schema, table, keyword, page, size);
        return ResponseEntity.ok(result);
    }

    /**
     * 按schema搜索（在指定schema的所有表中搜索）
     *
     * @param schema  数据库schema
     * @param keyword 搜索关键字
     * @param page    页码（从0开始）
     * @param size    每页大小
     * @return 搜索结果
     */
    @GetMapping("/schema")
    public ResponseEntity<SearchResult> searchBySchema(
            @RequestParam String schema,
            @RequestParam(required = false) String keyword,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        if (page < 0) page = 0;
        if (size <= 0 || size > 100) size = 20;

        SearchResult result = searchService.searchAllTables(keyword, schema, null, page, size);
        return ResponseEntity.ok(result);
    }

    /**
     * 获取可用的索引列表
     *
     * @return 索引列表
     */
    @GetMapping("/indices")
    public ResponseEntity<List<String>> getAvailableIndices() {
        List<String> indices = searchService.getAvailableIndices();
        return ResponseEntity.ok(indices);
    }

    /**
     * 高级搜索接口
     *
     * @param request 搜索请求
     * @return 搜索结果
     */
    @PostMapping("/advanced")
    public ResponseEntity<SearchResult> advancedSearch(@RequestBody AdvancedSearchRequest request) {
        // 验证请求参数
        if (request.getPage() < 0) request.setPage(0);
        if (request.getSize() <= 0 || request.getSize() > 100) request.setSize(20);

        SearchResult result = searchService.searchAllTables(
            request.getKeyword(),
            request.getSchema(),
            request.getTable(),
            request.getPage(),
            request.getSize()
        );

        return ResponseEntity.ok(result);
    }

    private List<SearchResult.SearchHit> compactHits(List<SearchResult.SearchHit> hits) {
        if (hits == null || hits.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> prefixes = resolveStatementPrefixes();
        List<SearchResult.SearchHit> compact = new ArrayList<SearchResult.SearchHit>(hits.size());

        for (SearchResult.SearchHit original : hits) {
            if (original == null) {
                continue;
            }

            SearchResult.SearchHit minimal = new SearchResult.SearchHit();
            minimal.setIndex(original.getIndex());
            minimal.setId(stripStatementPrefix(original.getId(), prefixes));
            compact.add(minimal);
        }
        return compact;
    }

    private List<String> resolveStatementPrefixes() {
        if (kingBaseSqlSyncService == null) {
            return Collections.emptyList();
        }

        Set<String> keys = kingBaseSqlSyncService.getConfiguredStatementKeys();
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> prefixes = new ArrayList<String>(keys);
        prefixes.sort((left, right) -> Integer.compare(
            right != null ? right.length() : 0,
            left != null ? left.length() : 0
        ));
        return prefixes;
    }

    private String stripStatementPrefix(String documentId, List<String> prefixes) {
        if (documentId == null || prefixes == null || prefixes.isEmpty()) {
            return documentId;
        }

        for (String prefix : prefixes) {
            if (prefix == null || prefix.isEmpty()) {
                continue;
            }
            String expected = prefix + "_";
            if (documentId.startsWith(expected)) {
                return documentId.substring(expected.length());
            }
        }
        return documentId;
    }

    /**
     * 高级搜索请求体
     */
    public static class AdvancedSearchRequest {
        private String keyword;
        private String schema;
        private String table;
        private int page = 0;
        private int size = 20;

        // Getters and Setters
        public String getKeyword() {
            return keyword;
        }

        public void setKeyword(String keyword) {
            this.keyword = keyword;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public int getPage() {
            return page;
        }

        public void setPage(int page) {
            this.page = page;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }
    }
}
