package org.kreps.redistoiotdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceConfig {
    @JsonProperty("redis")
    private RedisSettings redisSettings;

    @JsonProperty("tags_file")
    private String tagsFile;

    // Getters
    public RedisSettings getRedisSettings() {
        return redisSettings;
    }

    public String getTagsFile() {
        return tagsFile;
    }

    public void validate() throws ConfigValidationException {
        if (redisSettings == null) {
            throw new ConfigValidationException("'source.redis' section is missing");
        }
        if (tagsFile == null || tagsFile.isEmpty()) {
            throw new ConfigValidationException("'source.tags_file' is missing or empty");
        }

        redisSettings.validate();
    }
}
