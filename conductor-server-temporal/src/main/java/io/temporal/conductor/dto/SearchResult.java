package io.temporal.conductor.dto;

import java.util.List;

/**
 * Generic search result container with pagination support.
 *
 * @param <T> The type of items in the result
 */
public class SearchResult<T> {
    private long totalHits;
    private List<T> results;

    public SearchResult() {
    }

    public SearchResult(long totalHits, List<T> results) {
        this.totalHits = totalHits;
        this.results = results;
    }

    public long getTotalHits() {
        return totalHits;
    }

    public void setTotalHits(long totalHits) {
        this.totalHits = totalHits;
    }

    public List<T> getResults() {
        return results;
    }

    public void setResults(List<T> results) {
        this.results = results;
    }
}
