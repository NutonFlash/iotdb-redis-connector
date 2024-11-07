package org.kreps.redistoiotdb.threading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ThreadPoolManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolManager.class);
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

    private final ExecutorService writerPool;
    private final CountDownLatch writerCompletionLatch;
    private volatile boolean isShutdown = false;

    public ThreadPoolManager(int writerPoolSize) {
        if (writerPoolSize <= 0) {
            throw new IllegalArgumentException("Writer pool size must be positive");
        }
        this.writerPool = Executors.newFixedThreadPool(writerPoolSize);
        this.writerCompletionLatch = new CountDownLatch(writerPoolSize);
        logger.info("ThreadPoolManager initialized with {} writers", writerPoolSize);
    }

    public ExecutorService getWriterPool() {
        if (isShutdown) {
            throw new IllegalStateException("ThreadPoolManager is shut down");
        }
        return writerPool;
    }

    public CountDownLatch getWriterLatch() {
        return writerCompletionLatch;
    }

    public void waitForWriters() throws InterruptedException {
        logger.info("Waiting for writers to complete...");
        boolean completed = writerCompletionLatch.await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!completed) {
            logger.warn("Timeout waiting for writers to complete");
        } else {
            logger.info("All writers completed");
        }
    }

    @Override
    public synchronized void close() {
        if (isShutdown) {
            logger.debug("ThreadPoolManager already shut down");
            return;
        }

        isShutdown = true;
        logger.info("Shutting down writer thread pool...");

        writerPool.shutdown();
        try {
            // Wait for writers to finish
            if (!writerPool.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                logger.warn("Writer pool didn't terminate in time, forcing shutdown");
                writerPool.shutdownNow();

                // Wait one more time for tasks to respond to interruption
                if (!writerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.error("Writer pool did not terminate");
                }
            }
        } catch (InterruptedException e) {
            logger.error("Shutdown interrupted, forcing immediate shutdown");
            writerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("Writer thread pool shutdown completed");
    }
}
