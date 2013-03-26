package org.elasticsearch.river.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

/**
 * User: andyolliver
 * Date: 19/03/2013
 * Time: 22:00
 * To change this template use File | Settings | File Templates.
 */
public class QueueingConsumerFactory {
    public QueueingConsumer newConsumer(Channel channel) {
        return new QueueingConsumer(channel);
    }
}
