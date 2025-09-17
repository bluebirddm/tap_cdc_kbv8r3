package com.tapdata.cdc.search;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SearchResult {

    private long totalHits;
    private Double maxScore;
    private long duration;
    private List<SearchHit> hits;

    public long getTotalHits() {
        return totalHits;
    }

    public void setTotalHits(long totalHits) {
        this.totalHits = totalHits;
    }

    public Double getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(Double maxScore) {
        this.maxScore = maxScore;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public List<SearchHit> getHits() {
        return hits;
    }

    public void setHits(List<SearchHit> hits) {
        this.hits = hits;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SearchHit {
        private String index;
        private String id;
        private Double score;
        private Map<String, Object> source;
        private Map<String, List<String>> highlight;

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Double getScore() {
            return score;
        }

        public void setScore(Double score) {
            this.score = score;
        }

        public Map<String, Object> getSource() {
            return source;
        }

        public void setSource(Map<String, Object> source) {
            this.source = source;
        }

        public Map<String, List<String>> getHighlight() {
            return highlight;
        }

        public void setHighlight(Map<String, List<String>> highlight) {
            this.highlight = highlight;
        }
    }
}
