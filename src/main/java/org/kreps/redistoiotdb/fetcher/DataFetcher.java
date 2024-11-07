package org.kreps.redistoiotdb.fetcher;

import org.kreps.redistoiotdb.config.AppConfig;
import org.kreps.redistoiotdb.model.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.asynchttpclient.*;

public class DataFetcher implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DataFetcher.class);
    private static final String PWCM_CD = "ST";
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");

    private final AppConfig config;
    private final BlockingQueue<DataPoint> dataQueue;
    private final ObjectMapper objectMapper;
    private final AsyncHttpClient httpClient;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean isRunning;
    private volatile long nextFetchTime;

    public DataFetcher(AppConfig config, BlockingQueue<DataPoint> dataQueue) {
        this.config = config;
        this.dataQueue = dataQueue;
        this.objectMapper = new ObjectMapper();
        this.httpClient = Dsl.asyncHttpClient();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.isRunning = new AtomicBoolean(false);
    }

    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            int intervalMs = config.getProcessingConfig().getFetcher().getIntervalMs();

            nextFetchTime = System.currentTimeMillis();
            logger.info("Initial fetch scheduled for: {}",
                    Instant.ofEpochMilli(nextFetchTime)
                            .atZone(ZoneId.systemDefault())
                            .format(TIME_FORMATTER));

            scheduler.scheduleAtFixedRate(
                    () -> {
                        fetchData();
                        nextFetchTime = System.currentTimeMillis() + intervalMs;
                        logger.info("Next fetch scheduled for: {}",
                                Instant.ofEpochMilli(nextFetchTime)
                                        .atZone(ZoneId.systemDefault())
                                        .format(TIME_FORMATTER));
                    },
                    0,
                    intervalMs,
                    TimeUnit.MILLISECONDS);

            logger.info("DataFetcher started with interval: {} ms", intervalMs);
        }
    }

    private void fetchData() {
        try {
            String url = buildApiRequest();
            logger.debug("Fetching data from URL: {}", url);

            httpClient
                    .prepareGet(url)
                    .setRequestTimeout(config.getProcessingConfig().getFetcher().getTimeoutMs())
                    .execute(new FetchCallback(dataQueue, objectMapper))
                    .toCompletableFuture();
        } catch (Exception e) {
            logger.error("Failed to initiate data fetch: {}", e.getMessage());
        }
    }

    private String buildApiRequest() {
        String apiUrl = config.getSourceConfig().getRedisSettings().getApiUrl();
        String userKey = config.getSourceConfig().getRedisSettings().getUserKey();
        List<String> tags = config.getTags();

        return apiUrl + "?tags=" + String.join(",", tags) +
                "&PWCM_CD=" + PWCM_CD +
                "&USER_KEY=" + userKey;
    }

    @Override
    public void close() {
        if (isRunning.compareAndSet(true, false)) {
            try {
                scheduler.shutdown();
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
                httpClient.close();
            } catch (Exception e) {
                logger.error("Error while closing DataFetcher", e);
                scheduler.shutdownNow();
            }
            logger.info("DataFetcher stopped");
        }
    }
}
