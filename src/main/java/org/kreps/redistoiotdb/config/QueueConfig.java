package org.kreps.redistoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QueueConfig {
    @JsonProperty("capacity")
    private int capacity;

    public int getCapacity() {
        return capacity;
    }

    public void validate() throws ConfigValidationException {
        if (capacity <= 0) {
            throw new ConfigValidationException("'processing.queue.capacity' must be greater than 0");
        }
    }
}   