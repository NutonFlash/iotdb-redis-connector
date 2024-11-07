package org.kreps.redistoiotdb.fetcher;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.HttpResponseHeaders;
import org.kreps.redistoiotdb.model.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class FetchCallback implements AsyncHandler<Void> {
    private static final Logger logger = LoggerFactory.getLogger(FetchCallback.class);
    private final BlockingQueue<DataPoint> dataQueue;
    private final ObjectMapper objectMapper;
    private final StringBuilder responseBuilder;
    private int statusCode;

    public FetchCallback(BlockingQueue<DataPoint> dataQueue, ObjectMapper objectMapper) {
        this.dataQueue = dataQueue;
        this.objectMapper = objectMapper;
        this.responseBuilder = new StringBuilder();
    }

    @Override
    public State onStatusReceived(HttpResponseStatus responseStatus) {
        this.statusCode = responseStatus.getStatusCode();
        if (statusCode != 200) {
            logger.error("Received error status code: {}", statusCode);
            return State.ABORT;
        }
        return State.CONTINUE;
    }

    @Override
    public State onHeadersReceived(HttpResponseHeaders headers) {
        return State.CONTINUE;
    }

    @Override
    public State onBodyPartReceived(HttpResponseBodyPart bodyPart) {
        try {
            responseBuilder.append(new String(bodyPart.getBodyPartBytes()));
            return State.CONTINUE;
        } catch (Exception e) {
            logger.error("Error processing body part: {}", e.getMessage());
            return State.ABORT;
        }
    }

    @Override
    public Void onCompleted() {
        try {
            if (statusCode == 200) {
                String response = responseBuilder.toString();
                List<Map<String, String>> dataList = objectMapper.readValue(
                        response,
                        new TypeReference<List<Map<String, String>>>() {
                        });

                int processedCount = 0;
                int droppedCount = 0;

                for (Map<String, String> data : dataList) {
                    try {
                        DataPoint dataPoint = new DataPoint(data);
                        if (dataQueue.offer(dataPoint)) {
                            processedCount++;
                        } else {
                            droppedCount++;
                        }
                    } catch (Exception e) {
                        logger.error("Error processing data point: {}", e.getMessage());
                    }
                }

                logger.info("Processed {} data points, dropped {} due to queue full",
                        processedCount, droppedCount);
            }
        } catch (Exception e) {
            logger.error("Error processing response: {}", e.getMessage());
        }
        return null;
    }

    @Override
    public void onThrowable(Throwable t) {
        logger.error("Request failed: {}", t.getMessage());
    }
}
