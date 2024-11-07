package org.kreps.redistoiotdb.worker;

import org.kreps.redistoiotdb.config.AppConfig;
import org.kreps.redistoiotdb.model.DataPoint;
import org.kreps.redistoiotdb.fetcher.DataFetcher;
import org.kreps.redistoiotdb.iotdb.IoTDBSessionPool;
import org.kreps.redistoiotdb.writer.IoTDBWriter;
import org.kreps.redistoiotdb.threading.ThreadPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class WorkerManager {
    private static final Logger logger = LoggerFactory.getLogger(WorkerManager.class);

    private final AppConfig config;
    private final BlockingQueue<DataPoint> dataQueue;
    private final ThreadPoolManager threadPoolManager;
    private final IoTDBSessionPool iotdbSessionPool;
    private final List<IoTDBWriter> writers = new ArrayList<>();
    private DataFetcher fetcher;
    private volatile boolean shutdownInProgress = false;

    public WorkerManager(AppConfig config, BlockingQueue<DataPoint> dataQueue,
            ThreadPoolManager threadPoolManager, IoTDBSessionPool iotdbSessionPool) {
        this.config = config;
        this.dataQueue = dataQueue;
        this.threadPoolManager = threadPoolManager;
        this.iotdbSessionPool = iotdbSessionPool;
    }

    public void startWorkers() {
        if (shutdownInProgress) {
            throw new IllegalStateException("Cannot start workers during shutdown");
        }
        startWriters();
        startFetcher();
        logger.info("All workers started successfully");
    }

    private void startWriters() {
        int writerPoolSize = config.getProcessingConfig().getWriter().getPoolSize();
        logger.info("Starting {} writer threads...", writerPoolSize);

        for (int i = 0; i < writerPoolSize; i++) {
            IoTDBWriter writer = new IoTDBWriter(
                    config,
                    dataQueue,
                    iotdbSessionPool,
                    threadPoolManager.getWriterLatch(),
                    this,
                    i + 1);
            writers.add(writer);
            threadPoolManager.getWriterPool().submit(writer);
        }
        logger.info("All writer threads started");
    }

    private void startFetcher() {
        logger.info("Starting data fetcher...");
        fetcher = new DataFetcher(config, dataQueue);
        fetcher.start();
        logger.info("Data fetcher started");
    }

    public synchronized void initiateShutdown() {
        if (shutdownInProgress) {
            logger.info("Shutdown already in progress");
            return;
        }

        shutdownInProgress = true;
        logger.info("Initiating graceful shutdown...");

        // First stop the fetcher to prevent new data from being added
        if (fetcher != null) {
            logger.info("Stopping data fetcher...");
            fetcher.close();
            logger.info("Data fetcher stopped");
        }

        // Then send poison pills to writers
        sendPoisonPills();
    }

    private void sendPoisonPills() {
        if (writers.isEmpty()) {
            logger.warn("No writers to send poison pills to");
            return;
        }

        logger.info("Sending poison pills to {} writers...", writers.size());
        for (int i = 0; i < writers.size(); i++) {
            try {
                dataQueue.put(DataPoint.POISON_PILL);
                logger.debug("Sent poison pill {}/{}", i + 1, writers.size());
            } catch (InterruptedException e) {
                logger.error("Interrupted while sending poison pills", e);
                Thread.currentThread().interrupt();
                break;
            }
        }
        logger.info("All poison pills sent");
    }
}