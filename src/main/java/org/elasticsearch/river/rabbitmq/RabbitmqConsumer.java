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
    private Connection connection;
    private ESLogger logger;
    private RabbitmqRiverConfigHolder configHolder;
    private Channel channel;
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
            logger.info("setting up connection and channel");
            connectionFactory.setUsername(configHolder.getRabbitUser());
            connectionFactory.setPassword(configHolder.getRabbitPassword());
            connectionFactory.setVirtualHost(configHolder.getRabbitVhost());

            try {
                connection = connectionFactory.newConnection(configHolder.getRabbitAddresses());
                channel = connection.createChannel();
            } catch (Exception e) {
                if (!closed) {
                    logger.warn("failed to created a connection / channel", e);
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

            QueueingConsumer consumer = queueingConsumerFactory.newConsumer(channel);
            // define the queue
            try {
                channel.exchangeDeclare(configHolder.getRabbitExchange(),
                        configHolder.getRabbitExchangeType(),
                        configHolder.isRabbitExchangeDurable());
                channel.queueDeclare(configHolder.getRabbitQueue(),
                        configHolder.isRabbitQueueDurable(),
                        false/*exclusive*/,
                        configHolder.isRabbitQueueAutoDelete(),
                        configHolder.getRabbitQueueArgs());
                channel.queueBind(configHolder.getRabbitQueue(),
                        configHolder.getRabbitExchange(),
                        configHolder.getRabbitRoutingKey());
                channel.basicConsume(configHolder.getRabbitQueue(),
                        false/*noAck*/,
                        consumer);
            } catch (Exception e) {
                if (!closed) {
                    logger.warn("failed to create queue [{}]", e, configHolder.getRabbitQueue());
                    cleanup(0, "failed to create queue");
                }
                continue;
            }

            // now use the queue to listen for indexing messages
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
                logger.debug("message collection loop - start cycle");
                QueueingConsumer.Delivery task = null;
                try {
                    task = consumer.nextDelivery();
                } catch (Exception e) {
                    // break out of this loop by throwing exception
                    throw new RabbitmqMessageProcessingException("failed to collect message", e);
                }

                if (task != null && task.getBody() != null) {
                    final List<Long> deliveryTags = Lists.newArrayList();

                    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

                    try {
                        bulkRequestBuilder.add(task.getBody(), 0, task.getBody().length, false);
                    } catch (Exception e) {
                        logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getEnvelope().getDeliveryTag());
                        try {
                            channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
                        } catch (IOException e1) {
                            logger.warn("failed to ack [{}]", e1, task.getEnvelope().getDeliveryTag());
                        }
                        continue;
                    }

                    deliveryTags.add(task.getEnvelope().getDeliveryTag());

                    if (bulkRequestBuilder.numberOfActions() < configHolder.getBulkSize()) {
                        // try and spin some more of those without timeout, so we have a bigger bulk (bounded by the bulk size)
                        try {
                            while ((task = consumer.nextDelivery(configHolder.getBulkTimeout().millis())) != null) {
                                try {
                                    bulkRequestBuilder.add(task.getBody(), 0, task.getBody().length, false);
                                    deliveryTags.add(task.getEnvelope().getDeliveryTag());
                                } catch (Exception e) {
                                    logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getEnvelope().getDeliveryTag());
                                    try {
                                        channel.basicAck(task.getEnvelope().getDeliveryTag(), false);
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
                            if (response.hasFailures()) {
                                // TODO write to exception queue?
                                logger.warn("failed to execute" + response.buildFailureMessage());
                            }
                            for (Long deliveryTag : deliveryTags) {
                                try {
                                    channel.basicAck(deliveryTag, false);
                                } catch (Exception e1) {
                                    logger.warn("failed to ack [{}]", e1, deliveryTag);
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("failed to execute bulk", e);
                        }
                    } else {
                        bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                            @Override
                            public void onResponse(BulkResponse response) {
                                if (response.hasFailures()) {
                                    // TODO write to exception queue?
                                    logger.warn("failed to execute" + response.buildFailureMessage());
                                }
                                for (Long deliveryTag : deliveryTags) {
                                    try {
                                        channel.basicAck(deliveryTag, false);
                                    } catch (Exception e1) {
                                        logger.warn("failed to ack [{}]", e1, deliveryTag);
                                    }
                                }
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                logger.warn("failed to execute bulk for delivery tags [{}], not ack'ing", e, deliveryTags);
                            }
                        });
                    }
                }
            }
        } finally {
            processingMessages = false;
        }
    }

    private void cleanup(int code, String message) {
        if (channel != null) {
            try {
                channel.close(code, message);
            } catch (AlreadyClosedException e) {
                logger.debug("failed to close channel on [{}] - [{}]", message, e.getLocalizedMessage());
            } catch (Exception e) {
                logger.debug("failed to close channel on [{}]", e, message);
            }
        }
        if (connection != null) {
            try {
                connection.close(code, message);
            } catch (AlreadyClosedException e) {
                logger.debug("failed to close channel on [{}] - [{}]", message, e.getLocalizedMessage());
            } catch (Exception e) {
                logger.debug("failed to close connection on [{}]", e, message);
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
}
