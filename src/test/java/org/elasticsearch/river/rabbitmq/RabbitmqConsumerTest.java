package org.elasticsearch.river.rabbitmq;

import com.rabbitmq.client.*;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.mockito.InOrder;
import org.mockito.MockitoAnnotations.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.to;
import static com.jayway.awaitility.Awaitility.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;


public class RabbitmqConsumerTest {

    ESLogger logger = Loggers.getLogger("test");
    @Mock  Client mockClient;
    @Mock ConnectionFactory mockConnectionFactory;
    @Mock Connection mockConnection;
    @Mock Channel mockChannel;
    @Mock QueueingConsumerFactory mockQueueingConsumerFactory;
    @Mock QueueingConsumer mockQueueingConsumer;
    @Mock BulkRequestBuilder mockBulkRequestBuilder;
    @Mock ListenableActionFuture<BulkResponse> mockListenableActionFuture;
    @Mock BulkResponse mockBulkResponse;

    QueueingConsumer.Delivery okMessage;
    QueueingConsumer.Delivery emptyMessage;
    QueueingConsumer.Delivery goodMessage;
    QueueingConsumer.Delivery badMessage;
    Counter message_counter;

    @BeforeMethod
    public void setup() throws IOException {
        System.out.println("start setup");
        Loggers.enableConsoleLogging();

        initMocks(this);

        when(mockConnectionFactory.newConnection(any(Address[].class))).thenReturn(mockConnection);
        when(mockConnection.createChannel()).thenReturn(mockChannel);
        when(mockQueueingConsumerFactory.newConsumer(mockChannel)).thenReturn(mockQueueingConsumer);

        okMessage = new QueueingConsumer.Delivery(null, null, "xxx".getBytes());
        emptyMessage = new QueueingConsumer.Delivery(null, null, null);
        goodMessage = new QueueingConsumer.Delivery(new Envelope(10l, false, "test", ""), null, "{}".getBytes());
        //  public Envelope(long deliveryTag, boolean redeliver, String exchange, String routingKey) {

        badMessage = new QueueingConsumer.Delivery(new Envelope(10l, false, "test", ""), null, "xx yy zz".getBytes());
        //  public Delivery(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        message_counter = new Counter();
    }

    // class to use for counting messages received
    private class Counter {
        // start stepping through the array from the beginning
        private int count = 0;

        public int getCount() {
            return count;
        }

        public void increment() {
            count++;
        }
    }

    private static RabbitmqConsumer initialiseRabbitmqConsumer(ESLogger logger, RabbitmqRiverConfigHolder configHolder, Client mockClient, ConnectionFactory mockConnectionFactory, QueueingConsumerFactory mockQueueingConsumerFactory) {
        RabbitmqConsumer rabbitmqConsumer = new RabbitmqConsumer();
        rabbitmqConsumer.setLogger(logger);
        rabbitmqConsumer.setConfig(configHolder);
        rabbitmqConsumer.setClient(mockClient);
        rabbitmqConsumer.setConnectionFactory(mockConnectionFactory);
        rabbitmqConsumer.setQueueingConsumerFactory(mockQueueingConsumerFactory);
        return rabbitmqConsumer;
    }

    /**
     * Mocks everything to do with Rabbitmq client.
     * Sends 'null message' so no message parsing or BulkRequestBuilder activity happens
     * Tests that:
     * - the mq channel is set up correctly
     * - 1 message is picked up
     * - the mq channel and connection closed
     */
    @Test
    public void testConnectCollect1MessageThenClose() throws IOException, InterruptedException {
        System.out.println("start testConnectCollect1MessageThenClose");
        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                new HashMap<String, Object>(),
                new HashMap<String, Object>());

        final RabbitmqConsumer rabbitmqConsumer = initialiseRabbitmqConsumer(
                logger,
                configHolder,
                mockClient,
                mockConnectionFactory,
                mockQueueingConsumerFactory);

        when(mockQueueingConsumer.nextDelivery()).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                message_counter.increment();
                // close after 1st message collected
                rabbitmqConsumer.setClosed();
                return null;
            }
        });

        Thread thread = new Thread(rabbitmqConsumer);
        thread.start();

        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));

        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

        InOrder inOrder = inOrder(mockConnectionFactory, mockConnection, mockChannel, mockQueueingConsumerFactory, mockQueueingConsumer);

        inOrder.verify(mockConnectionFactory).setUsername(configHolder.getRabbitUser());
        inOrder.verify(mockConnectionFactory).setPassword(configHolder.getRabbitPassword());
        inOrder.verify(mockConnectionFactory).setVirtualHost(configHolder.getRabbitVhost());
        inOrder.verify(mockConnectionFactory).newConnection(configHolder.getRabbitAddresses());
        inOrder.verify(mockConnection).createChannel();
        inOrder.verify(mockQueueingConsumerFactory).newConsumer(mockChannel);
        inOrder.verify(mockChannel).exchangeDeclare(configHolder.getRabbitExchange(), configHolder.getRabbitExchangeType(), configHolder.isRabbitExchangeDurable());
        inOrder.verify(mockChannel).queueDeclare(configHolder.getRabbitQueue(), configHolder.isRabbitQueueDurable(), false, configHolder.isRabbitQueueAutoDelete(), configHolder.getRabbitQueueArgs());
        inOrder.verify(mockChannel).queueBind(configHolder.getRabbitQueue(), configHolder.getRabbitExchange(), configHolder.getRabbitRoutingKey());
        inOrder.verify(mockChannel).basicConsume(configHolder.getRabbitQueue(), false, mockQueueingConsumer);

        inOrder.verify(mockQueueingConsumer).nextDelivery();

        inOrder.verify(mockChannel).close(eq(0), any(String.class));
        inOrder.verify(mockConnection).close(eq(0), any(String.class));

        inOrder.verifyNoMoreInteractions();
        assertThat(message_counter.getCount(), equalTo(1));
    }


    /**
     * Mocks everything to do with Rabbitmq client.
     * Request to connect to RabbitMq fails x2, then succeeds.
     * Tests that:
     * - at start-up will retry connecting to Rabbitmq untill success
     */
    @Test
    public void testAttemptConnectTillSuccess() throws IOException, InterruptedException {
        System.out.println("start testConnectCollect1MessageThenClose");
        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                new HashMap<String, Object>(),
                new HashMap<String, Object>());

        ConnectionFactory failTwiceBeforeConnectConnectionFactory = mock(ConnectionFactory.class);

        final RabbitmqConsumer rabbitmqConsumer = initialiseRabbitmqConsumer(
                logger,
                configHolder,
                mockClient,
                failTwiceBeforeConnectConnectionFactory,
                mockQueueingConsumerFactory);

        when(mockQueueingConsumer.nextDelivery()).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                message_counter.increment();
                // close after 1st message collected
                rabbitmqConsumer.setClosed();
                return null;
            }
        });

        // set up mock ConnectionFactory to fail x2 before providing good connection.
        when(failTwiceBeforeConnectConnectionFactory.newConnection(configHolder.getRabbitAddresses())).thenThrow(new IOException()).thenThrow(new IOException()).thenReturn(mockConnection);

        Thread thread = new Thread(rabbitmqConsumer);
        thread.start();

        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));

        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

        InOrder inOrder = inOrder(failTwiceBeforeConnectConnectionFactory, mockConnection, mockChannel, mockQueueingConsumerFactory, mockQueueingConsumer);

        for ( int i=0; i<3; i++) {
            inOrder.verify(failTwiceBeforeConnectConnectionFactory).setUsername(configHolder.getRabbitUser());
            inOrder.verify(failTwiceBeforeConnectConnectionFactory).setPassword(configHolder.getRabbitPassword());
            inOrder.verify(failTwiceBeforeConnectConnectionFactory).setVirtualHost(configHolder.getRabbitVhost());
            inOrder.verify(failTwiceBeforeConnectConnectionFactory).newConnection(configHolder.getRabbitAddresses());
        }

        inOrder.verify(mockConnection).createChannel();
        inOrder.verify(mockQueueingConsumerFactory).newConsumer(mockChannel);
        inOrder.verify(mockChannel).exchangeDeclare(configHolder.getRabbitExchange(), configHolder.getRabbitExchangeType(), configHolder.isRabbitExchangeDurable());
        inOrder.verify(mockChannel).queueDeclare(configHolder.getRabbitQueue(), configHolder.isRabbitQueueDurable(), false, configHolder.isRabbitQueueAutoDelete(), configHolder.getRabbitQueueArgs());
        inOrder.verify(mockChannel).queueBind(configHolder.getRabbitQueue(), configHolder.getRabbitExchange(), configHolder.getRabbitRoutingKey());
        inOrder.verify(mockChannel).basicConsume(configHolder.getRabbitQueue(), false, mockQueueingConsumer);

        inOrder.verify(mockQueueingConsumer).nextDelivery();

        inOrder.verify(mockChannel).close(eq(0), any(String.class));
        inOrder.verify(mockConnection).close(eq(0), any(String.class));

        inOrder.verifyNoMoreInteractions();
        assertThat(message_counter.getCount(), equalTo(1));
    }


    /**
     * Mocks everything to do with Rabbitmq client.
     * Sends 'null message' so no message parsing or BulkRequestBuilder activity happens
     * bulk_size = 1  ==> single message per loop.
     * Tests that:
     * - the mq channel is set up correctly
     * - 3 message are picked up
     * - the mq channel and connection closed
     */
    @Test
    public void testCollects3MessagesThenClose() throws IOException, InterruptedException {
        System.out.println("start testCollects3MessagesThenClose");

        Map<String, Object> rabbitSettings = new HashMap<String, Object>();
        Map<String, Object> indexSettings = new HashMap<String, Object>();

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                indexSettings);

        final RabbitmqConsumer rabbitmqConsumer = initialiseRabbitmqConsumer(
                logger,
                configHolder,
                mockClient,
                mockConnectionFactory,
                mockQueueingConsumerFactory);

        when(mockQueueingConsumer.nextDelivery()).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                message_counter.increment();
                // close after 3rd message collected
                if (message_counter.getCount() == 3) {
                    rabbitmqConsumer.setClosed();
                }
                return null;
            }
        });

        Thread thread = new Thread(rabbitmqConsumer);
        thread.start();
        // wait for it to pick up 3 messages

        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 3 messages processed")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(3));

        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

        InOrder inOrder = inOrder(mockConnection, mockChannel, mockQueueingConsumer);

        inOrder.verify(mockQueueingConsumer, times(3)).nextDelivery();
        inOrder.verify(mockChannel).close(eq(0), any(String.class));
        inOrder.verify(mockConnection).close(eq(0), any(String.class));

        inOrder.verifyNoMoreInteractions();
        assertThat(message_counter.getCount(), equalTo(3));
    }

    /**
     * Mocks everything to do with Rabbitmq client.
     * Sends real message now we use the BulkRequestBuilder
     * bulk_size = 1  ==> so no attempts to concatenate multiple mesasages
     * Tests that:
     * - the mq channel is set up correctly
     * - 3 message are picked up
     * - the mq channel and connection closed
     */
    @Test
    public void testProcessGoodMessage() throws Exception {
        System.out.println("start testProcessGoodMessage");

        Map<String, Object> rabbitSettings = new HashMap<String, Object>();
        Map<String, Object> indexSettings = new HashMap<String, Object>();
        indexSettings.put("bulk_size", 1);

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                indexSettings);

        final RabbitmqConsumer rabbitmqConsumer = initialiseRabbitmqConsumer(
                logger,
                configHolder,
                mockClient,
                mockConnectionFactory,
                mockQueueingConsumerFactory);

        when(mockQueueingConsumer.nextDelivery()).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                message_counter.increment();
                rabbitmqConsumer.setClosed();
                return goodMessage;
            }
        });

        when(mockClient.prepareBulk()).thenReturn(mockBulkRequestBuilder);

        Thread thread = new Thread(rabbitmqConsumer);
        thread.start();

        // wait for it to pick up 1 message
        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));
        // wait for work to stop
        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

        InOrder inOrder = inOrder(mockConnection, mockChannel, mockQueueingConsumer, mockClient, mockBulkRequestBuilder);

        inOrder.verify(mockQueueingConsumer, times(1)).nextDelivery();
        inOrder.verify(mockClient, times(1)).prepareBulk();
        inOrder.verify(mockBulkRequestBuilder, times(1)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
        inOrder.verify(mockChannel).close(eq(0), any(String.class));
        inOrder.verify(mockConnection).close(eq(0), any(String.class));

        inOrder.verifyNoMoreInteractions();
        assertThat(message_counter.getCount(), equalTo(1));
    }

    /**
     * Mocks everything to do with Rabbitmq client, and ES Client.
     * Sends real message now we use the BulkRequestBuilder
     * bulk_size = 1  ==> so no attempts to concatenate multiple messages
     * Tests that:
     * - the mq channel is set up correctly
     * - 3 message are picked up
     * - 3 BulkRequest created and executed using the synchronous method
     * - 3 acks returned to Rabbitmq
     * - the mq channel and connection closed
     */
    @Test
    public void testProcess3GoodMessagesOrderedTrue() throws Exception {
        System.out.println("start testProcess3GoodMessagesOrderedTrue");
        //  public RabbitmqRiverConfigHolder(Map<String, Object> rabbitSettings, Map<String, Object> indexSettings)
        Map<String, Object> rabbitSettings = new HashMap<String, Object>();
        Map<String, Object> indexSettings = new HashMap<String, Object>();
        indexSettings.put("bulk_size", 1);
        // indexSettings.put("bulk_timeout","");
        indexSettings.put("ordered", true);

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                indexSettings);

        final RabbitmqConsumer rabbitmqConsumer = initialiseRabbitmqConsumer(
                logger,
                configHolder,
                mockClient,
                mockConnectionFactory,
                mockQueueingConsumerFactory);

        when(mockQueueingConsumer.nextDelivery()).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                message_counter.increment();
                // close after 3rd message collected
                if (message_counter.getCount() == 3) {
                    rabbitmqConsumer.setClosed();
                }
                return goodMessage;
            }
        });

        when(mockClient.prepareBulk()).thenReturn(mockBulkRequestBuilder);
        when(mockBulkRequestBuilder.numberOfActions()).thenReturn(1);
        when(mockBulkRequestBuilder.execute()).thenReturn(mockListenableActionFuture);
        when(mockListenableActionFuture.actionGet()).thenReturn(mockBulkResponse);

        Thread thread = new Thread(rabbitmqConsumer);
        thread.start();

        // wait for it to pick up 1 message
        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 3 messages processed")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(3));
        // wait for work to stop
        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

        assertThat(message_counter.getCount(), equalTo(3));

        verify(mockQueueingConsumer, times(3)).nextDelivery();
        verify(mockClient, times(3)).prepareBulk();
        verify(mockBulkRequestBuilder, times(3)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
        verify(mockBulkRequestBuilder, times(3)).execute();
        verify(mockChannel, times(3)).basicAck(10l, false);

        verify(mockChannel).close(eq(0), any(String.class));
        verify(mockConnection).close(eq(0), any(String.class));

        // we're not using the async method
        verify(mockBulkRequestBuilder, never()).execute((ActionListener<BulkResponse>) any(ActionListener.class));
    }


    /**
     * Mocks everything to do with Rabbitmq client, and ES Client.
     * Sends real message now we use the BulkRequestBuilder
     * bulk_size = 1  ==> so no attempts to concatenate multiple messages
     * Tests that:
     * - the mq channel is set up correctly
     * - 3 message are picked up
     * - 3 BulkRequest created and executed using the asynchronous method
     * - 3 acks returned to Rabbitmq
     * - the mq channel and connection closed
     */
    @Test
    public void testProcess3GoodMessagesOrderedFalse() throws Exception {
        System.out.println("start testProcess3GoodMessagesOrderedFalse");
        Map<String, Object> rabbitSettings = new HashMap<String, Object>();
        Map<String, Object> indexSettings = new HashMap<String, Object>();
        indexSettings.put("bulk_size", 1);

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                indexSettings);

        final RabbitmqConsumer rabbitmqConsumer = initialiseRabbitmqConsumer(
                logger,
                configHolder,
                mockClient,
                mockConnectionFactory,
                mockQueueingConsumerFactory);

        when(mockQueueingConsumer.nextDelivery()).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                message_counter.increment();
                // close after 3rd message collected
                if (message_counter.getCount() == 3) {
                    rabbitmqConsumer.setClosed();
                }
                return goodMessage;
            }
        });

        when(mockClient.prepareBulk()).thenReturn(mockBulkRequestBuilder);
        when(mockBulkRequestBuilder.numberOfActions()).thenReturn(1);
        when(mockBulkRequestBuilder.execute()).thenReturn(mockListenableActionFuture);
        when(mockListenableActionFuture.actionGet()).thenReturn(mockBulkResponse);

        Thread thread = new Thread(rabbitmqConsumer);
        thread.start();

        // wait for it to pick up 1 message
        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 3 messages processed")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(3));
        // wait for work to stop
        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

        assertThat(message_counter.getCount(), equalTo(3));

        verify(mockQueueingConsumer, times(3)).nextDelivery();
        verify(mockClient, times(3)).prepareBulk();
        verify(mockBulkRequestBuilder, times(3)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
        verify(mockBulkRequestBuilder, times(3)).execute((ActionListener<BulkResponse>) any(ActionListener.class));

        //TODO - ack now called by the ActionListener - need a bit more thinking to test this..
        // verify(mockChannel, times(3)).basicAck(10l, false);

        verify(mockChannel).close(eq(0), any(String.class));
        verify(mockConnection).close(eq(0), any(String.class));

        // we're not using the async method
        verify(mockBulkRequestBuilder, never()).execute();
    }

    /**
     * Mocks everything to do with Rabbitmq client, and ES Client
     * Sends real message now we use the BulkRequestBuilder
     * bulk_size = 10  ==> so we attempt to concatenate multiple messages
     * Tests that:
     * - the mq channel is set up correctly
     * - 3 message are picked up
     * - only one BulkRequest created and executed using the synchronous method
     * - 3 acks returned to Rabbitmq
     * - the mq channel and connection closed
     */
    @Test
    public void testProcess3GoodMessagesInSingleBulkExecutionOrderedTrue() throws Exception {
        System.out.println("start testProcess3GoodMessagesInSingleBulkExecutionOrderedTrue");

        Map<String, Object> rabbitSettings = new HashMap<String, Object>();
        Map<String, Object> indexSettings = new HashMap<String, Object>();
        indexSettings.put("bulk_size", 10);
        indexSettings.put("bulk_timeout", "1000");
        indexSettings.put("ordered", true);

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                indexSettings);

        final RabbitmqConsumer rabbitmqConsumer = initialiseRabbitmqConsumer(
                logger,
                configHolder,
                mockClient,
                mockConnectionFactory,
                mockQueueingConsumerFactory);

        when(mockQueueingConsumer.nextDelivery()).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                message_counter.increment();
                return goodMessage;
            }
        });

        when(mockQueueingConsumer.nextDelivery(1000)).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                message_counter.increment();
                // close after 3rd message collected
                if (message_counter.getCount() == 3) {
                    rabbitmqConsumer.setClosed();
                }
                return goodMessage;
            }
        });

        when(mockClient.prepareBulk()).thenReturn(mockBulkRequestBuilder);
        when(mockBulkRequestBuilder.execute()).thenReturn(mockListenableActionFuture);
        when(mockListenableActionFuture.actionGet()).thenReturn(mockBulkResponse);

        Thread thread = new Thread(rabbitmqConsumer);
        thread.start();

        // wait for it to pick up 1 message
        with().pollInterval(50, TimeUnit.MILLISECONDS).and().pollDelay(50, TimeUnit.MILLISECONDS).await("until 3 messages processed")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(3));
        // wait for work to stop
        with().pollInterval(50, TimeUnit.MILLISECONDS).and().pollDelay(50, TimeUnit.MILLISECONDS).await("until no longer running")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

        assertThat(message_counter.getCount(), equalTo(3));

        verify(mockQueueingConsumer).nextDelivery();
        verify(mockClient).prepareBulk();
        verify(mockQueueingConsumer, times(2)).nextDelivery(1000);
        verify(mockBulkRequestBuilder, times(3)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
        verify(mockBulkRequestBuilder).execute();
        verify(mockChannel, times(3)).basicAck(10l, false);

        verify(mockChannel).close(eq(0), any(String.class));
        verify(mockConnection).close(eq(0), any(String.class));

        // we're not using the async method
        verify(mockBulkRequestBuilder, never()).execute((ActionListener<BulkResponse>) any(ActionListener.class));
    }

    /**
     * Mocks everything to do with Rabbitmq client, and ES Client
     * Sends real message now we use the BulkRequestBuilder
     * bulk_size = 10  ==> so we attempt to concatenate multiple messages
     * Tests that:
     * - the mq channel is set up correctly
     * - 3 message are picked up
     * - only one BulkRequest created and executed using the asynchronous method
     * - 3 acks returned to Rabbitmq
     * - the mq channel and connection closed
     */
    @Test
    public void testProcess3GoodMessagesInSingleBulkExecutionOrderedFalse() throws Exception {
        System.out.println("start testProcess3GoodMessagesInSingleBulkExecutionOrderedFalse");

        Map<String, Object> rabbitSettings = new HashMap<String, Object>();

        Map<String, Object> indexSettings = new HashMap<String, Object>();
        indexSettings.put("bulk_size", 10);
        indexSettings.put("bulk_timeout", "1000");
        indexSettings.put("ordered", false);

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                indexSettings);

        final RabbitmqConsumer rabbitmqConsumer = initialiseRabbitmqConsumer(
                logger,
                configHolder,
                mockClient,
                mockConnectionFactory,
                mockQueueingConsumerFactory);

        when(mockQueueingConsumer.nextDelivery()).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                message_counter.increment();
                return goodMessage;
            }
        });

        when(mockQueueingConsumer.nextDelivery(1000)).thenAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                message_counter.increment();
                // close after 3rd message collected
                if (message_counter.getCount() == 3) {
                    rabbitmqConsumer.setClosed();
                }
                return goodMessage;
            }
        });

        when(mockClient.prepareBulk()).thenReturn(mockBulkRequestBuilder);
        when(mockBulkRequestBuilder.execute()).thenReturn(mockListenableActionFuture);
        when(mockListenableActionFuture.actionGet()).thenReturn(mockBulkResponse);

        Thread thread = new Thread(rabbitmqConsumer);
        thread.start();

        // wait for it to pick up 1 message
        with().pollInterval(50, TimeUnit.MILLISECONDS).and().pollDelay(50, TimeUnit.MILLISECONDS).await("until 3 messages processed")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(3));
        // wait for work to stop
        with().pollInterval(50, TimeUnit.MILLISECONDS).and().pollDelay(50, TimeUnit.MILLISECONDS).await("until no longer running")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

        assertThat(message_counter.getCount(), equalTo(3));

        verify(mockQueueingConsumer).nextDelivery();
        verify(mockClient).prepareBulk();
        verify(mockQueueingConsumer, times(2)).nextDelivery(1000);
        verify(mockBulkRequestBuilder, times(3)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
        verify(mockBulkRequestBuilder).execute((ActionListener<BulkResponse>) any(ActionListener.class));

        //TODO - ack now called by the ActionListener - need a bit more thinking to test this..
        //verify(mockChannel, times(3)).basicAck(10l, false);

        verify(mockChannel).close(eq(0), any(String.class));
        verify(mockConnection).close(eq(0), any(String.class));

        // we're not using the sync method
        verify(mockBulkRequestBuilder, never()).execute();
    }


}
