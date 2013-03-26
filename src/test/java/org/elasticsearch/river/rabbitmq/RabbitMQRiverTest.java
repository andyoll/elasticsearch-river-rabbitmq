package org.elasticsearch.river.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.sun.corba.se.impl.logging.ORBUtilSystemException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.with;
import static org.mockito.Mockito.*;

/**
 * User: andyolliver
 * Date: 26/02/2013
 */
public class RabbitmqRiverTest {

    @BeforeClass
    public void oneTimeSetUp() {
        Loggers.enableConsoleLogging();
    }

    @Test
    public void textSetUpAndRunConsumer() throws IOException, InterruptedException {
        RiverName riverName = new RiverName("rabbitmq", "rabbitmq");
        Settings globalSettings = ImmutableSettings.settingsBuilder().build();
        Map riverSettingsMap = new HashMap<String, Object>();
        RiverSettings riverSettings = new RiverSettings(globalSettings, riverSettingsMap);

        Client mockClient = mock(Client.class);
        final RabbitmqRiver rabbitmqRiver = new RabbitmqRiver(riverName, riverSettings, mockClient);
        RabbitmqConsumerFactory mockRabbitmqConsumerFactory = mock(RabbitmqConsumerFactory.class);
        final RabbitmqConsumer mockRabbitmqConsumer = mock(RabbitmqConsumer.class);

        when(mockRabbitmqConsumerFactory.newConsumer()).thenReturn(mockRabbitmqConsumer);

        rabbitmqRiver.setRabbitmqConsumerFactory(mockRabbitmqConsumerFactory);
        rabbitmqRiver.start();

        InOrder inOrder = inOrder(mockRabbitmqConsumerFactory, mockRabbitmqConsumer);

        inOrder.verify(mockRabbitmqConsumerFactory).newConsumer();
        inOrder.verify(mockRabbitmqConsumer).setLogger(any(ESLogger.class));
        inOrder.verify(mockRabbitmqConsumer).setConfig(any(RabbitmqRiverConfigHolder.class));
        inOrder.verify(mockRabbitmqConsumer).setConnectionFactory(any(ConnectionFactory.class));
        inOrder.verify(mockRabbitmqConsumer).setQueueingConsumerFactory(any(QueueingConsumerFactory.class));
        inOrder.verify(mockRabbitmqConsumer).setClient(mockClient);
        inOrder.verify(mockRabbitmqConsumer).run();
        inOrder.verifyNoMoreInteractions();

        rabbitmqRiver.close();

        verify(mockRabbitmqConsumer).setClosed();
        verifyNoMoreInteractions(mockRabbitmqConsumer);

    }

}
