package org.elasticsearch.river.rabbitmq;

/**
 * User: andyolliver
 * Date: 19/03/2013
 * Time: 20:09
 * To change this template use File | Settings | File Templates.
 */
public class RabbitmqMessageProcessingException extends Exception {
    public RabbitmqMessageProcessingException(String msg, Exception e) {
        super(msg, e);
    }
}
