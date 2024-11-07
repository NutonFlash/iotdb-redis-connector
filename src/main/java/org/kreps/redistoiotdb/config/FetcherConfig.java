package org.kreps.redistoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FetcherConfig {
    @JsonProperty("interval_ms")
    private int intervalMs;

    @JsonProperty("timeout_ms")
    private int timeoutMs;

    public int getIntervalMs() {
        return intervalMs;
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }

    public void validate() throws ConfigValidationException {
        if (intervalMs <= 0) {
            throw new ConfigValidationException("'processing.fetcher.interval_ms' must be greater than 0");
        }
        if (timeoutMs <= 0) {
            throw new ConfigValidationException("'processing.fetcher.timeout_ms' must be greater than 0");
        }
    }
}