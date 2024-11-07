package org.kreps.redistoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WriterConfig {
    @JsonProperty("pool_size")
    private int poolSize;

    @JsonProperty("batch_size")
    private int batchSize;

    public int getPoolSize() { return poolSize; }
    public int getBatchSize() { return batchSize; }

    public void validate() throws ConfigValidationException {
        if (poolSize <= 0) {
            throw new ConfigValidationException("'processing.writer.pool_size' must be greater than 0");
        }
        if (batchSize <= 0) {
            throw new ConfigValidationException("'processing.writer.batch_size' must be greater than 0");
        }
    }
} 