package org.kreps.redistoiotdb.config;

public class ProcessingConfig {
    private WriterConfig writer;
    private QueueConfig queue;
    private FetcherConfig fetcher;

    // Getters
    public WriterConfig getWriter() {
        return writer;
    }

    public QueueConfig getQueue() {
        return queue;
    }

    public FetcherConfig getFetcher() {
        return fetcher;
    }

    public void validate() throws ConfigValidationException {
        // Validate existence of config objects
        if (writer == null) {
            throw new ConfigValidationException("'processing.writer' configuration is missing");
        }
        if (queue == null) {
            throw new ConfigValidationException("'processing.queue' configuration is missing");
        }
        if (fetcher == null) {
            throw new ConfigValidationException("'processing.fetcher' configuration is missing");
        }

        // Validate each config object
        writer.validate();
        queue.validate();
        fetcher.validate();
    }
}
