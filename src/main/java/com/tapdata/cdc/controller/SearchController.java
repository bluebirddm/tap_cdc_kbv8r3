package com.tapdata.cdc.controller;

import com.tapdata.cdc.search.SearchResult;
import com.tapdata.cdc.search.SearchService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/search")
@CrossOrigin(origins = "*")
public class SearchController {

    private final SearchService searchService;

    public SearchController(SearchService searchService) {
        this.searchService = searchService;
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
