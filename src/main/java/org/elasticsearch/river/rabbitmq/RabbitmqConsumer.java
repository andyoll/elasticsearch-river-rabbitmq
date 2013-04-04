package org.elasticsearch.river.rabbitmq;

import com.rabbitmq.client.*;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.logging.ESLogger;

import java.io.IOException;
import java.util.List;

class RabbitmqConsumer implements Runnable {

    private ConnectionFactory connectionFactory;
    private Connection indexingConnection;
    private Connection responseConnection;
    private ESLogger logger;
    private RabbitmqRiverConfigHolder configHolder;
    private Channel indexingChannel;
    private Channel responseChannel;
    private volatile boolean closed = false;
    private volatile boolean running = false;
    private volatile boolean processingMessages = false;
    private Client client;
    private QueueingConsumerFactory queueingConsumerFactory;

    @Override
    public void run() {

        while (true) {
            if (closed) {
                break;
            }
            running = true;

            logger.info("setting up connection and channel for indexing messages");
            connectionFactory.setUsername(configHolder.getIndexUser());
            connectionFactory.setPassword(configHolder.getIndexPassword());
            connectionFactory.setVirtualHost(configHolder.getIndexVhost());
            try {
                indexingConnection = connectionFactory.newConnection(configHolder.getIndexAddresses());
                indexingChannel = indexingConnection.createChannel();
            } catch (Exception e) {
                if (!closed) {
                    logger.warn("failed to created a connection / channel for indexing messages", e);
                    cleanup(0, "failed to connect");
                    try {
                        // wait before re-entering loop and attempting to reconnect
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        // ignore, if we are closing, we will exit later
                    }
                }
                continue;
            }

            QueueingConsumer consumer = queueingConsumerFactory.newConsumer(indexingChannel);
            // define the queue
            try {
                indexingChannel.exchangeDeclare(configHolder.getIndexExchange(),
                        configHolder.getIndexExchangeType(),
                        configHolder.isIndexExchangeDurable());
                indexingChannel.queueDeclare(configHolder.getIndexQueue(),
                        configHolder.isIndexQueueDurable(),
                        false/*exclusive*/,
                        configHolder.isIndexQueueAutoDelete(),
                        configHolder.getIndexQueueArgs());
                indexingChannel.queueBind(configHolder.getIndexQueue(),
                        configHolder.getIndexExchange(),
                        configHolder.getIndexRoutingKey());
                indexingChannel.basicConsume(configHolder.getIndexQueue(),
                        false/*noAck*/,
                        consumer);
            } catch (Exception e) {
                if (!closed) {
                    logger.warn("failed to create queue [{}]", e, configHolder.getIndexQueue());
                    cleanup(0, "failed to create index queue");
                }
                continue;
            }


            if (configHolder.isResponseMessagesEnabled()) {
                logger.info("setting up connection and channel for response messages");
                connectionFactory.setUsername(configHolder.getResponseUser());
                connectionFactory.setPassword(configHolder.getResponsePassword());
                connectionFactory.setVirtualHost(configHolder.getResponseVhost());
                try {
                    responseConnection = connectionFactory.newConnection(configHolder.getResponseAddresses());
                    responseChannel = responseConnection.createChannel();
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to created a connection or channel for response messages", e);
                        cleanup(0, "failed to connect");
                        try {
                            // wait before re-entering loop and attempting to reconnect
                            Thread.sleep(5000);
                        } catch (InterruptedException e1) {
                            // ignore, if we are closing, we will exit later
                        }
                    }
                    continue;
                }

                // define the queue
                try {
                    responseChannel.exchangeDeclare(configHolder.getResponseExchange(),
                            configHolder.getResponseExchangeType(),
                            configHolder.isResponseExchangeDurable());
                    responseChannel.queueDeclare(configHolder.getResponseQueue(),
                            configHolder.isResponseQueueDurable(),
                            false/*exclusive*/,
                            configHolder.isResponseQueueAutoDelete(),
                            configHolder.getResponseQueueArgs());
                    responseChannel.queueBind(configHolder.getResponseQueue(),
                            configHolder.getResponseExchange(),
                            configHolder.getIndexRoutingKey());
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to create queue [{}]", e, configHolder.getResponseQueue());
                        cleanup(0, "failed to create response queue");
                    }
                    continue;
                }
            }

            // now use the indexing queue to listen for indexing messages
            try {
                processIndexingMessages(consumer);
            } catch (RabbitmqMessageProcessingException e) {
                if (!closed) {
                    logger.error("failed to get next message, reconnecting...", e);
                    cleanup(0, e.getMessage());
                }
            }
        }
        cleanup(0, "closing river");
        running = false;
    }

    void processIndexingMessages(QueueingConsumer consumer) throws RabbitmqMessageProcessingException {
        logger.info("start processing messages");
        try {
            while (true) {
                if (closed) {
                    break;
                }
                processingMessages = true;
                QueueingConsumer.Delivery task = null;
                try {
                    task = consumer.nextDelivery();
                } catch (Exception e) {
                    // break out of this loop by throwing exception
                    throw new RabbitmqMessageProcessingException("failed to collect message", e);
                }

                if (task != null && task.getBody() != null) {
                    logger.debug("message received - add to new BulkRequestBuilder");
                    final List<IndexingMsgDetail> indexingMsgDetails = Lists.newArrayList();

                    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

                    try {
                        bulkRequestBuilder.add(task.getBody(), 0, task.getBody().length, false);
                    } catch (Exception e) {
                        logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getEnvelope().getDeliveryTag());
                        if (configHolder.isResponseMessagesEnabled()) {
                            // if this call to sendResponseMessage(..) fails, then it throws exception, which breaks us out of this loop
                            sendResponseMessage(task.getProperties().getCorrelationId(), false, new String(task.getBody()), new FaultInfo(e));
                        }
                        try {
                            indexingChannel.basicAck(task.getEnvelope().getDeliveryTag(), false);
                        } catch (IOException e1) {
                            logger.warn("failed to ack [{}]", e1, task.getEnvelope().getDeliveryTag());
                        }
                        continue;
                    }

                    indexingMsgDetails.add(new IndexingMsgDetail(task));

                    if (bulkRequestBuilder.numberOfActions() < configHolder.getBulkSize()) {
                        // try and spin some more of those without timeout, so we have a bigger bulk (bounded by the bulk size)
                        try {
                            while ((task = consumer.nextDelivery(configHolder.getBulkTimeout().millis())) != null) {
                                logger.debug("message received - add to existing BulkRequestBuilder");
                                try {
                                    bulkRequestBuilder.add(task.getBody(), 0, task.getBody().length, false);
                                    indexingMsgDetails.add(new IndexingMsgDetail(task));
                                } catch (Exception e) {
                                    logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getEnvelope().getDeliveryTag());
                                    if (configHolder.isResponseMessagesEnabled()) {
                                        // if fails, then throws exception, which breaks us out of this loop
                                        sendResponseMessage(task.getProperties().getCorrelationId(), false, new String(task.getBody()), new FaultInfo(e));
                                    }
                                    try {
                                        indexingChannel.basicAck(task.getEnvelope().getDeliveryTag(), false);
                                    } catch (Exception e1) {
                                        logger.warn("failed to ack on failure [{}]", e1, task.getEnvelope().getDeliveryTag());
                                    }
                                }
                                if (bulkRequestBuilder.numberOfActions() >= configHolder.getBulkSize() || closed) {
                                    break;
                                }
                            }
                        } catch (Exception e) {
                            // break out of this loop by throwing exception - this will trigger a cleanup.
                            throw new RabbitmqMessageProcessingException("failed to collect message", e);
                        }
                    }

                    if (logger.isTraceEnabled()) {
                        logger.trace("executing bulk with [{}] actions", bulkRequestBuilder.numberOfActions());
                    }

                    if (configHolder.isOrdered()) {
                        try {
                            BulkResponse response = bulkRequestBuilder.execute().actionGet();
                            String failureMsg = null;
                            if (response.hasFailures()) {
                                failureMsg = response.buildFailureMessage();
                                logger.warn("failed to execute" + failureMsg);
                            }

                            for (IndexingMsgDetail indexingMsgDetail : indexingMsgDetails) {
                                if (configHolder.isResponseMessagesEnabled()) {
                                    if (response.hasFailures()) {
                                        // bulk response has errors
                                        sendResponseMessage(indexingMsgDetail.getCorrelationId(), false, new String(indexingMsgDetail.getBulkRequest()), new FaultInfo("BulkResponse.Failures", failureMsg));
                                    } else {
                                        // bulk response all ok
                                        sendResponseMessage(indexingMsgDetail.getCorrelationId(), true, new String(indexingMsgDetail.getBulkRequest()), null);
                                    }
                                }
                                try {
                                    indexingChannel.basicAck(indexingMsgDetail.getDeliveryTag(), false);
                                } catch (Exception e1) {
                                    logger.warn("failed to ack [{}]", e1, indexingMsgDetail.getDeliveryTag());
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("failed to execute bulk", e);
                            for (IndexingMsgDetail indexingMsgDetail : indexingMsgDetails) {
                                if (configHolder.isResponseMessagesEnabled()) {
                                    sendResponseMessage(indexingMsgDetail.getCorrelationId(), false, new String(indexingMsgDetail.getBulkRequest()), new FaultInfo(e));
                                }
                            }
                        }
                    } else {
                        bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                            @Override
                            public void onResponse(BulkResponse response) {
                                String failureMsg = null;
                                if (response.hasFailures()) {
                                    failureMsg = response.buildFailureMessage();
                                    logger.warn("failed to execute: " + failureMsg);
                                }
                                for (IndexingMsgDetail indexingMsgDetail : indexingMsgDetails) {
                                    if (configHolder.isResponseMessagesEnabled()) {
                                        try {
                                            if (response.hasFailures()) {
                                                // bulk response has errors
                                                sendResponseMessage(indexingMsgDetail.getCorrelationId(), false, new String(indexingMsgDetail.getBulkRequest()), new FaultInfo("BulkResponse.Failures", failureMsg));
                                            } else {
                                                // bulk response all ok
                                                sendResponseMessage(indexingMsgDetail.getCorrelationId(), true, new String(indexingMsgDetail.getBulkRequest()), null);
                                            }
                                        } catch (RabbitmqMessageProcessingException e) {
                                            logger.warn("failed to send response message for CorrelationId [{}]", indexingMsgDetail.getCorrelationId());
                                        }
                                    }
                                    try {
                                        indexingChannel.basicAck(indexingMsgDetail.getDeliveryTag(), false);
                                    } catch (Exception e1) {
                                        logger.warn("failed to ack [{}]", e1, indexingMsgDetail.getDeliveryTag());
                                    }
                                }
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                List<Long> deliveryTags = Lists.newArrayList();
                                for (IndexingMsgDetail indexingMsgDetail : indexingMsgDetails) {
                                    deliveryTags.add(indexingMsgDetail.getDeliveryTag());
                                    if (configHolder.isResponseMessagesEnabled()) {
                                        try {
                                            sendResponseMessage(indexingMsgDetail.getCorrelationId(), false, new String(indexingMsgDetail.getBulkRequest()), new FaultInfo(e));
                                        } catch (RabbitmqMessageProcessingException e1) {
                                            logger.warn("failed to send response message for CorrelationId [{}]", indexingMsgDetail.getCorrelationId());
                                        }
                                    }
                                }
                                logger.warn("failed to execute bulk for delivery tags [{}], not ack'ing", e, deliveryTags);
                            }
                        });
                    }
                } else {
                    logger.debug("null or empty message received - nothing to do");
                }
            }
        } finally {
            processingMessages = false;
        }
    }

    private void sendResponseMessage(String correlationId, Boolean success, String bulkRequest, FaultInfo faultInfo) throws RabbitmqMessageProcessingException {
        // create msg properties;
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        AMQP.BasicProperties msgProps = builder.correlationId(correlationId).appId(configHolder.getResponseAppId()).build();

        // create msg body:
        ResponseMessageBody responseMessageBody = new ResponseMessageBody();
        responseMessageBody.setSuccess(success);
        responseMessageBody.setFaultInfo(faultInfo);
        responseMessageBody.setBulkRequest(bulkRequest);

        try {
            logger.debug("sending response message: correlation_id: " + correlationId);
            logger.debug("response message content: " + responseMessageBody.toJson());
            responseChannel.basicPublish(
                    configHolder.getResponseExchange(),
                    configHolder.getResponseRoutingKey(),
                    msgProps,
                    responseMessageBody.toJson().getBytes());
        } catch (IOException e1) {
            throw new RabbitmqMessageProcessingException("failed to send response message", e1);
        }
    }

    private void cleanup(int code, String message) {
        logger.debug("cleanup all connections and channels");
        if (indexingChannel != null) {
            try {
                indexingChannel.close(code, message);
            } catch (AlreadyClosedException e) {
                logger.debug("failed to close indexing channel on [{}] - [{}]", message, e.getLocalizedMessage());
            } catch (Exception e) {
                logger.debug("failed to close indexing channel on [{}]", e, message);
            }
        }

        if (indexingConnection != null) {
            try {
                indexingConnection.close(code, message);
            } catch (AlreadyClosedException e) {
                logger.debug("failed to close indexingConnection on [{}] - [{}]", message, e.getLocalizedMessage());
            } catch (Exception e) {
                logger.debug("failed to close indexingConnection on [{}]", e, message);
            }
        }

        if (responseChannel != null) {
            try {
                responseChannel.close(code, message);
            } catch (AlreadyClosedException e) {
                logger.debug("failed to close response channel on [{}] - [{}]", message, e.getLocalizedMessage());
            } catch (Exception e) {
                logger.debug("failed to close response channel on [{}]", e, message);
            }
        }

        if (responseConnection != null) {
            try {
                responseConnection.close(code, message);
            } catch (AlreadyClosedException e) {
                logger.debug("failed to close responseConnection on [{}] - [{}]", message, e.getLocalizedMessage());
            } catch (Exception e) {
                logger.debug("failed to close responseConnection on [{}]", e, message);
            }
        }

    }

    void setLogger(ESLogger logger) {
        this.logger = logger;
    }

    void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    void setQueueingConsumerFactory(QueueingConsumerFactory factory) {
        this.queueingConsumerFactory = factory;
    }

    void setClosed() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isProcessingMessages() {
        return processingMessages;
    }

    void setConfig(RabbitmqRiverConfigHolder configHolder) {
        this.configHolder = configHolder;
    }

    void setClient(Client client) {
        this.client = client;
    }

    private class IndexingMsgDetail {
        Long deliveryTag;
        String correlationId;
        byte[] bulkRequest;

        private IndexingMsgDetail(QueueingConsumer.Delivery task) {
            this.deliveryTag = task.getEnvelope().getDeliveryTag();
            this.correlationId = task.getProperties().getCorrelationId();
            this.bulkRequest = task.getBody();
        }

        public Long getDeliveryTag() {
            return deliveryTag;
        }

        public String getCorrelationId() {
            return correlationId;
        }

        public byte[] getBulkRequest() {
            return bulkRequest;
        }
    }
}
