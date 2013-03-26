package org.elasticsearch.river.rabbitmq;

import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

public class RabbitmqConsumerFactoryTest {

    @Test
    public void newConsumerTest() {
        RabbitmqConsumerFactory factory = new RabbitmqConsumerFactory();
        RabbitmqConsumer consumer = factory.newConsumer();
        assertNotNull(consumer, "RabbitmqConsumer should not be null");
    }

}
