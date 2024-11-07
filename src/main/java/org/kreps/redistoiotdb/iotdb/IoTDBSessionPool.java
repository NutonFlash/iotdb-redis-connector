package org.kreps.redistoiotdb.iotdb;

import org.apache.iotdb.session.pool.SessionPool;
import org.kreps.redistoiotdb.config.AppConfig;
import org.kreps.redistoiotdb.config.IoTDBSettings;
import org.kreps.redistoiotdb.utils.RetryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicBoolean;

public class IoTDBSessionPool implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(IoTDBSessionPool.class);

    private SessionPool sessionPool;

    private static final int CONNECTION_CHECK_INTERVAL_MS = 5000;
    private volatile boolean isAvailable = true;
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);
    private Thread connectionMonitorThread;
    private final AppConfig config;

    public IoTDBSessionPool(AppConfig config) {
        logger.info("Initializing IoTDB SessionPool with pool size: {}",
                config.getDestinationConfig().getIotdbSettings().getSessionPoolSize());
        this.config = config;
        initializeSessionPool(config.getDestinationConfig().getIotdbSettings());
        startConnectionMonitor();
    }

    private void initializeSessionPool(IoTDBSettings settings) {
        try {
            this.sessionPool = new SessionPool.Builder()
                    .host(settings.getHost())
                    .port(settings.getPort())
                    .user(settings.getUsername())
                    .password(settings.getPassword())
                    .maxSize(settings.getSessionPoolSize())
                    .connectionTimeoutInMs(10000)
                    .build();
            isAvailable = true;
            logger.info("IoTDB SessionPool initialized successfully with host: {}, port: {}",
                    settings.getHost(), settings.getPort());
        } catch (Exception e) {
            isAvailable = false;
            logger.error("Failed to initialize IoTDB SessionPool: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private synchronized void reinitializeSessionPool() {
        logger.info("Reinitializing IoTDB SessionPool...");
        if (sessionPool != null) {
            try {
                sessionPool.close();
            } catch (Exception e) {
                logger.warn("Error while closing old session pool: {}", e.getMessage());
            }
        }

        IoTDBSettings settings = config.getDestinationConfig().getIotdbSettings();
        initializeSessionPool(settings);
    }

    public SessionPool getSessionPool() {
        return sessionPool;
    }

    public boolean checkConnection() {
        try {
            return RetryUtils.executeWithRetry(() -> {
                try {
                    if (sessionPool == null) {
                        logger.warn("Session pool is null, attempting to reinitialize...");
                        reinitializeSessionPool();
                        return false;
                    }

                    sessionPool.executeQueryStatement("show databases");
                    if (!isAvailable) {
                        logger.info("IoTDB connection restored");
                        isAvailable = true;
                    }
                    return true;
                } catch (Exception e) {
                    if (sessionPool != null && (e.getMessage().contains("Session pool is closed") ||
                            e.getMessage().contains("timeout to get a connection"))) {
                        logger.warn("Connection pool issue detected, attempting to reinitialize...");
                        reinitializeSessionPool();
                    }
                    throw e; // Let retry mechanism handle it
                }
            }, config.getRetryConfig(), "IoTDB connection check");
        } catch (Exception e) {
            if (isAvailable) {
                logger.error("IoTDB connection lost: {}", e.getMessage());
                isAvailable = false;
            }
            return false;
        }
    }

    public boolean isAvailable() {
        return isAvailable;
    }

    private void startConnectionMonitor() {
        connectionMonitorThread = new Thread(() -> {
            while (!shutdownInitiated.get()) {
                try {
                    if (!checkConnection()) {
                        logger.error("IoTDB connection is not available, will retry in {} ms",
                                CONNECTION_CHECK_INTERVAL_MS);
                    }
                    Thread.sleep(CONNECTION_CHECK_INTERVAL_MS);
                } catch (InterruptedException e) {
                    logger.info("Connection monitor thread interrupted, shutting down");
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            logger.info("Connection monitor thread stopped");
        }, "IoTDB-Connection-Monitor");
        connectionMonitorThread.setDaemon(true);
        connectionMonitorThread.start();
    }

    @Override
    public synchronized void close() {
        if (shutdownInitiated.compareAndSet(false, true)) {
            logger.info("Shutting down IoTDB SessionPool...");

            // First interrupt and wait for the monitor thread
            if (connectionMonitorThread != null) {
                connectionMonitorThread.interrupt();
                try {
                    connectionMonitorThread.join(5000); // Wait up to 5 seconds
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted while waiting for monitor thread to stop");
                }
            }

            // Then close the session pool
            if (sessionPool != null) {
                try {
                    sessionPool.close();
                    logger.info("IoTDB SessionPool closed successfully");
                } catch (Exception e) {
                    logger.error("Error closing IoTDB SessionPool", e);
                }
            }
        }
    }
}
