package org.kreps.redistoiotdb;

import org.kreps.redistoiotdb.config.AppConfig;
import org.kreps.redistoiotdb.config.ConfigLoader;
import org.kreps.redistoiotdb.config.ConfigValidationException;
import org.kreps.redistoiotdb.iotdb.IoTDBSessionPool;
import org.kreps.redistoiotdb.model.DataPoint;
import org.kreps.redistoiotdb.validator.SchemaValidator;
import org.kreps.redistoiotdb.threading.ThreadPoolManager;
import org.kreps.redistoiotdb.worker.WorkerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private final BlockingQueue<DataPoint> dataQueue;
    private final IoTDBSessionPool iotdbSessionPool;
    private final ThreadPoolManager threadPoolManager;
    private final WorkerManager workerManager;
    private final AppConfig config;

    public Main(AppConfig config) {
        this.dataQueue = new LinkedBlockingQueue<>(config.getProcessingConfig().getQueue().getCapacity());
        this.iotdbSessionPool = new IoTDBSessionPool(config);

        int writerPoolSize = config.getProcessingConfig().getWriter().getPoolSize();

        this.threadPoolManager = new ThreadPoolManager(writerPoolSize);
        this.workerManager = new WorkerManager(
                config,
                dataQueue,
                threadPoolManager,
                iotdbSessionPool);
        this.config = config;
    }

    public static void main(String[] args) {
        try {
            AppConfig config = ConfigLoader.loadConfig();
            Main app = new Main(config);
            app.run();
        } catch (ConfigValidationException e) {
            logger.error("Configuration error: {}", e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            logger.error("Application failed: ", e);
            System.exit(1);
        }
    }

    private void run() throws Exception {
        try {
            validateSchema();
            workerManager.startWorkers();
            waitForShutdownSignal();
        } catch (Exception e) {
            logger.error("Application error: ", e);
            throw e;
        } finally {
            cleanup();
        }
    }

    private void waitForShutdownSignal() {
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Received shutdown signal");
            shutdownLatch.countDown();
        }));

        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Application interrupted");
        }
    }

    private void validateSchema() throws Exception {
        logger.info("Validating IoTDB schema...");
        SchemaValidator validator = new SchemaValidator(
                iotdbSessionPool.getSessionPool(),
                config.getRetryConfig());
        validator.initializeSchema();
        logger.info("Schema validation completed");
    }

    private void cleanup() {
        logger.info("Starting application cleanup...");
        try {
            // First stop the workers
            workerManager.initiateShutdown();

            // Wait for writers to complete with a timeout
            try {
                threadPoolManager.waitForWriters();
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for writers to complete");
                Thread.currentThread().interrupt();
            }

            // Close thread pools
            threadPoolManager.close();

            // Finally close IoTDB connection
            iotdbSessionPool.close();

            logger.info("Application cleanup completed");
        } catch (Exception e) {
            logger.error("Error during cleanup", e);
        }
    }
}
