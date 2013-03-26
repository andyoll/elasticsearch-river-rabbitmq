package org.elasticsearch.river.rabbitmq;

/**
 *
 */
class RabbitmqConsumerFactory {

    public RabbitmqConsumer newConsumer() {
        return new RabbitmqConsumer();
    }

}