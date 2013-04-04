package org.elasticsearch.river.rabbitmq;

import com.rabbitmq.client.*;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.hamcrest.Matchers;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.to;
import static com.jayway.awaitility.Awaitility.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;


public class RabbitmqConsumerTest {

    ESLogger logger = Loggers.getLogger("test");

    @Mock
    Client mockClient;

    @Mock
    ConnectionFactory mockConnectionFactory;

    @Mock
    Connection mockIndexingConnection;

    @Mock
    Connection mockResponseConnection;

    @Mock
    Channel mockIndexingChannel;

    @Mock
    Channel mockResponseChannel;

    @Mock
    QueueingConsumerFactory mockQueueingConsumerFactory;

    @Mock
    QueueingConsumer mockQueueingConsumer;

    @Mock
    BulkRequestBuilder mockBulkRequestBuilder;

    @Mock
    ListenableActionFuture<BulkResponse> mockListenableActionFuture;

    @Mock
    BulkResponse mockBulkResponse;

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

        when(mockConnectionFactory.newConnection(any(Address[].class))).thenReturn(mockIndexingConnection);


        Address[] indexAddresses = new Address[]{new Address("localhost", AMQP.PROTOCOL.PORT)};
        when(mockConnectionFactory.newConnection(indexAddresses)).thenReturn(mockIndexingConnection);

        Address[] responseAddresses = new Address[]{new Address("host1", AMQP.PROTOCOL.PORT)};
        when(mockConnectionFactory.newConnection(responseAddresses)).thenReturn(mockResponseConnection);

        when(mockIndexingConnection.createChannel()).thenReturn(mockIndexingChannel);
        when(mockResponseConnection.createChannel()).thenReturn(mockResponseChannel);
        when(mockQueueingConsumerFactory.newConsumer(mockIndexingChannel)).thenReturn(mockQueueingConsumer);

        okMessage = new QueueingConsumer.Delivery(null, null, "xxx".getBytes());
        emptyMessage = new QueueingConsumer.Delivery(null, null, null);
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        AMQP.BasicProperties msgProps = builder.correlationId("#abc345").build();

        goodMessage = new QueueingConsumer.Delivery(new Envelope(10l, false, "test", ""), msgProps, "{}".getBytes());
        //  public Envelope(long deliveryTag, boolean redeliver, String exchange, String routingKey) {

        badMessage = new QueueingConsumer.Delivery(new Envelope(10l, false, "test", ""), msgProps, "xx yy zz".getBytes());
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

    private static RabbitmqConsumer initialiseRabbitmqConsumer(ESLogger logger, RabbitmqRiverConfigHolder configHolder, Client mockClient, ConnectionFactory connectionFactory, QueueingConsumerFactory queueingConsumerFactory) {
        RabbitmqConsumer rabbitmqConsumer = new RabbitmqConsumer();
        rabbitmqConsumer.setLogger(logger);
        rabbitmqConsumer.setConfig(configHolder);
        rabbitmqConsumer.setClient(mockClient);
        rabbitmqConsumer.setConnectionFactory(connectionFactory);
        rabbitmqConsumer.setQueueingConsumerFactory(queueingConsumerFactory);
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
    public void testConnectCollect1MessageThenCloseResponseDisabled() throws IOException, InterruptedException {
        System.out.println("start testConnectCollect1MessageThenClose");
        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                new HashMap<String, Object>(),
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

        InOrder inOrder = inOrder(mockConnectionFactory, mockIndexingConnection, mockIndexingChannel, mockQueueingConsumerFactory, mockQueueingConsumer);

        inOrder.verify(mockConnectionFactory).setUsername(configHolder.getIndexUser());
        inOrder.verify(mockConnectionFactory).setPassword(configHolder.getIndexPassword());
        inOrder.verify(mockConnectionFactory).setVirtualHost(configHolder.getIndexVhost());
        inOrder.verify(mockConnectionFactory).newConnection(configHolder.getIndexAddresses());
        inOrder.verify(mockIndexingConnection).createChannel();
        inOrder.verify(mockQueueingConsumerFactory).newConsumer(mockIndexingChannel);
        inOrder.verify(mockIndexingChannel).exchangeDeclare(configHolder.getIndexExchange(), configHolder.getIndexExchangeType(), configHolder.isIndexExchangeDurable());
        inOrder.verify(mockIndexingChannel).queueDeclare(configHolder.getIndexQueue(), configHolder.isIndexQueueDurable(), false, configHolder.isIndexQueueAutoDelete(), configHolder.getIndexQueueArgs());
        inOrder.verify(mockIndexingChannel).queueBind(configHolder.getIndexQueue(), configHolder.getIndexExchange(), configHolder.getIndexRoutingKey());
        inOrder.verify(mockIndexingChannel).basicConsume(configHolder.getIndexQueue(), false, mockQueueingConsumer);

        inOrder.verify(mockQueueingConsumer).nextDelivery();

        inOrder.verify(mockIndexingChannel).close(eq(0), any(String.class));
        inOrder.verify(mockIndexingConnection).close(eq(0), any(String.class));

        inOrder.verifyNoMoreInteractions();
        assertThat(message_counter.getCount(), equalTo(1));
    }


    /**
     * Mocks everything to do with Rabbitmq client.
     * Request to connect to RabbitMq fails x2, then succeeds.
     * Response messages not enabled
     * Tests that:
     * - at start-up will retry connecting to Rabbitmq untill success
     */
    @Test
    public void retryConnectingToIndexingChannelTillSuccess() throws IOException, InterruptedException {
        System.out.println("start testConnectCollect1MessageThenClose");
        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                new HashMap<String, Object>(),
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
        when(failTwiceBeforeConnectConnectionFactory.newConnection(configHolder.getIndexAddresses())).thenThrow(new IOException()).thenThrow(new IOException()).thenReturn(mockIndexingConnection);

        Thread thread = new Thread(rabbitmqConsumer);
        thread.start();

        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));

        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

        InOrder inOrder = inOrder(failTwiceBeforeConnectConnectionFactory, mockIndexingConnection, mockIndexingChannel, mockQueueingConsumerFactory, mockQueueingConsumer);

        for (int i = 0; i < 3; i++) {
            inOrder.verify(failTwiceBeforeConnectConnectionFactory).setUsername(configHolder.getIndexUser());
            inOrder.verify(failTwiceBeforeConnectConnectionFactory).setPassword(configHolder.getIndexPassword());
            inOrder.verify(failTwiceBeforeConnectConnectionFactory).setVirtualHost(configHolder.getIndexVhost());
            inOrder.verify(failTwiceBeforeConnectConnectionFactory).newConnection(configHolder.getIndexAddresses());
        }

        inOrder.verify(mockIndexingConnection).createChannel();
        inOrder.verify(mockQueueingConsumerFactory).newConsumer(mockIndexingChannel);
        inOrder.verify(mockIndexingChannel).exchangeDeclare(configHolder.getIndexExchange(), configHolder.getIndexExchangeType(), configHolder.isIndexExchangeDurable());
        inOrder.verify(mockIndexingChannel).queueDeclare(configHolder.getIndexQueue(), configHolder.isIndexQueueDurable(), false, configHolder.isIndexQueueAutoDelete(), configHolder.getIndexQueueArgs());
        inOrder.verify(mockIndexingChannel).queueBind(configHolder.getIndexQueue(), configHolder.getIndexExchange(), configHolder.getIndexRoutingKey());
        inOrder.verify(mockIndexingChannel).basicConsume(configHolder.getIndexQueue(), false, mockQueueingConsumer);

        inOrder.verify(mockQueueingConsumer).nextDelivery();

        inOrder.verify(mockIndexingChannel).close(eq(0), any(String.class));
        inOrder.verify(mockIndexingConnection).close(eq(0), any(String.class));

        inOrder.verifyNoMoreInteractions();
        assertThat(message_counter.getCount(), equalTo(1));
    }

    /**
     * Mocks everything to do with Rabbitmq client.
     * Request to connect to RabbitMq response channel fails x2, then succeeds.
     * Response messages enabled
     * Tests that:
     * - at start-up will retry connecting to Rabbitmq untill success
     */
    @Test
    public void retryConnectToResponseChannelTillSuccess() throws IOException, InterruptedException {
        System.out.println("start testConnectCollect1MessageThenClose");

        Map<String, Object> rabbitSettings = new HashMap<String, Object>();
        Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
        responseChannelSettings.put("response_messages_enabled", true);
        responseChannelSettings.put("host", "host1");

        Map<String, Object> indexSettings = new HashMap<String, Object>();

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                responseChannelSettings,
                indexSettings);

        ConnectionFactory unreliableConnectionFactory = mock(ConnectionFactory.class);

        final RabbitmqConsumer rabbitmqConsumer = initialiseRabbitmqConsumer(
                logger,
                configHolder,
                mockClient,
                unreliableConnectionFactory,
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

        // set up mock ConnectionFactory to provide good connection.
        when(unreliableConnectionFactory.newConnection(configHolder.getIndexAddresses())).thenReturn(mockIndexingConnection).thenReturn(mockIndexingConnection).thenReturn(mockIndexingConnection);

        // set up mock ConnectionFactory to fail x2 before providing good connection.
        when(unreliableConnectionFactory.newConnection(configHolder.getResponseAddresses())).thenThrow(new IOException()).thenThrow(new IOException()).thenReturn(mockResponseConnection);

        Thread thread = new Thread(rabbitmqConsumer);
        thread.start();

        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));

        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

        InOrder inOrder = inOrder(unreliableConnectionFactory, mockResponseConnection, mockResponseChannel);

        for (int i = 0; i < 3; i++) {
            inOrder.verify(unreliableConnectionFactory).setUsername(configHolder.getIndexUser());
            inOrder.verify(unreliableConnectionFactory).setPassword(configHolder.getIndexPassword());
            inOrder.verify(unreliableConnectionFactory).setVirtualHost(configHolder.getIndexVhost());
            inOrder.verify(unreliableConnectionFactory).newConnection(configHolder.getIndexAddresses());

            inOrder.verify(unreliableConnectionFactory).setUsername(configHolder.getResponseUser());
            inOrder.verify(unreliableConnectionFactory).setPassword(configHolder.getResponsePassword());
            inOrder.verify(unreliableConnectionFactory).setVirtualHost(configHolder.getResponseVhost());
            inOrder.verify(unreliableConnectionFactory).newConnection(configHolder.getResponseAddresses());
        }

        inOrder.verify(mockResponseConnection).createChannel();
        inOrder.verify(mockResponseChannel).exchangeDeclare(configHolder.getResponseExchange(), configHolder.getResponseExchangeType(), configHolder.isResponseExchangeDurable());
        inOrder.verify(mockResponseChannel).queueDeclare(configHolder.getResponseQueue(), configHolder.isResponseQueueDurable(), false, configHolder.isResponseQueueAutoDelete(), configHolder.getResponseQueueArgs());
        inOrder.verify(mockResponseChannel).queueBind(configHolder.getResponseQueue(), configHolder.getResponseExchange(), configHolder.getResponseRoutingKey());

        inOrder.verify(mockResponseChannel).close(eq(0), any(String.class));
        inOrder.verify(mockResponseConnection).close(eq(0), any(String.class));

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
    public void testCollects3MessagesThenCloseResponseDisabled() throws IOException, InterruptedException {
        System.out.println("start testCollects3MessagesThenClose");

        Map<String, Object> rabbitSettings = new HashMap<String, Object>();
        Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
        Map<String, Object> indexSettings = new HashMap<String, Object>();
        indexSettings.put("bulk_size", 1);

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                responseChannelSettings,
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

        InOrder inOrder = inOrder(mockIndexingConnection, mockIndexingChannel, mockQueueingConsumer);

        inOrder.verify(mockQueueingConsumer, times(3)).nextDelivery();
        inOrder.verify(mockIndexingChannel).close(eq(0), any(String.class));
        inOrder.verify(mockIndexingConnection).close(eq(0), any(String.class));

        inOrder.verifyNoMoreInteractions();
        assertThat(message_counter.getCount(), equalTo(3));
    }

    /**
     * Mocks everything to do with Rabbitmq client.
     * Sends real message now we use the BulkRequestBuilder
     * bulk_size = 1  ==> so no attempts to concatenate multiple mesasages
     * Tests that:
     * - 1 message is picked up
     * - message is added to bulkRequestBuilder
     * - bulkRequest is executed using async method
     * - bulkresponse is queried for failures
     * - an ack message is sent
     * - the mq channel and connection closed
     */
    @Test
    public void testAsyncBulkIndexingExecutionWithResponseDisabled() throws Exception {
        System.out.println("start testProcessGoodMessage");

        Map<String, Object> rabbitSettings = new HashMap<String, Object>();
        Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
        Map<String, Object> indexSettings = new HashMap<String, Object>();
        indexSettings.put("bulk_size", 1);

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                responseChannelSettings,
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
        final BulkResponse mockBulkResponse = mock(BulkResponse.class);
        when(mockBulkResponse.hasFailures()).thenReturn(false);

        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                ActionListener<BulkResponse> actionListener = (ActionListener<BulkResponse>) invocation.getArguments()[0];
                actionListener.onResponse(mockBulkResponse);
                return null;
            }
        }).when(mockBulkRequestBuilder).execute((ActionListener<BulkResponse>) any(ActionListener.class));


        Thread thread = new Thread(rabbitmqConsumer);
        thread.start();

        // wait for it to pick up 1 message
        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));
        // wait for work to stop
        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

        InOrder inOrder = inOrder(mockIndexingConnection, mockIndexingChannel, mockQueueingConsumer, mockClient, mockBulkRequestBuilder, mockBulkResponse);

        inOrder.verify(mockQueueingConsumer, times(1)).nextDelivery();
        inOrder.verify(mockClient, times(1)).prepareBulk();
        inOrder.verify(mockBulkRequestBuilder, times(1)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
        inOrder.verify(mockBulkRequestBuilder, times(1)).execute((ActionListener<BulkResponse>) any(ActionListener.class));
        inOrder.verify(mockBulkResponse).hasFailures();
        inOrder.verify(mockIndexingChannel).basicAck(10l, false);
        inOrder.verify(mockIndexingChannel).close(eq(0), any(String.class));
        inOrder.verify(mockIndexingConnection).close(eq(0), any(String.class));

        inOrder.verifyNoMoreInteractions();
        assertThat(message_counter.getCount(), equalTo(1));
    }


    /**
      * Mocks everything to do with Rabbitmq client.
      * Sends real message now we use the BulkRequestBuilder
      * bulk_size = 1  ==> so no attempts to concatenate multiple mesasages
      * Tests that:
      * - 1 message is picked up
      * - message is added to bulkRequestBuilder
      * - bulkRequest is executed using async method - response reports failures
      * - an ack message is sent - only witness to failure is lost in ES log
      * - the mq channel and connection closed
      */
     @Test
     public void testAsyncBulkIndexingExecutionWithTotalFailureWithResponseDisabled() throws Exception {
         System.out.println("start testProcessGoodMessage");

         Map<String, Object> rabbitSettings = new HashMap<String, Object>();
         Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
         Map<String, Object> indexSettings = new HashMap<String, Object>();
         indexSettings.put("bulk_size", 1);

         RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                 rabbitSettings,
                 responseChannelSettings,
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
         final BulkResponse mockBulkResponse = mock(BulkResponse.class);

         doAnswer(new Answer() {
             public Object answer(InvocationOnMock invocation) {
                 Object[] args = invocation.getArguments();
                 ActionListener<BulkResponse> actionListener = (ActionListener<BulkResponse>) invocation.getArguments()[0];
                 actionListener.onFailure(new Throwable("error: abc"));
                 return null;
             }
         }).when(mockBulkRequestBuilder).execute((ActionListener<BulkResponse>) any(ActionListener.class));


         Thread thread = new Thread(rabbitmqConsumer);
         thread.start();

         // wait for it to pick up 1 message
         with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                 .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));
         // wait for work to stop
         with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                 .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

         InOrder inOrder = inOrder(mockIndexingConnection, mockIndexingChannel, mockQueueingConsumer, mockClient, mockBulkRequestBuilder, mockBulkResponse);

         inOrder.verify(mockQueueingConsumer, times(1)).nextDelivery();
         inOrder.verify(mockClient, times(1)).prepareBulk();
         inOrder.verify(mockBulkRequestBuilder, times(1)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
         inOrder.verify(mockBulkRequestBuilder, times(1)).execute((ActionListener<BulkResponse>) any(ActionListener.class));
         inOrder.verify(mockIndexingChannel).close(eq(0), any(String.class));
         inOrder.verify(mockIndexingConnection).close(eq(0), any(String.class));

         inOrder.verifyNoMoreInteractions();
         assertThat(message_counter.getCount(), equalTo(1));
     }


    /**
      * Mocks everything to do with Rabbitmq client.
      * bulk_size = 1  ==> so no attempts to concatenate multiple mesasages
      * Tests that:
      * - the mq channel is set up correctly
      * - 3 message are picked up
      * - message is added to bulkRequestBuilder
      * - bulkRequest is executed using sync method
      * - bulkresponse is queried for failures
      * - an ack message is sent
      * - the mq channel and connection closed
      */
     @Test
     public void testSyncBulkIndexingExecutionWithResponseDisabled() throws Exception {
         System.out.println("start testProcessGoodMessage");

         Map<String, Object> rabbitSettings = new HashMap<String, Object>();
         Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
         Map<String, Object> indexSettings = new HashMap<String, Object>();
         indexSettings.put("bulk_size", 1);
         indexSettings.put("ordered", true);

         RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                 rabbitSettings,
                 responseChannelSettings,
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
         final BulkResponse mockBulkResponse = mock(BulkResponse.class);
         when(mockBulkResponse.hasFailures()).thenReturn(false);

         ListenableActionFuture<BulkResponse> mockListenableActionFuture = mock(ListenableActionFuture.class);
         when(mockListenableActionFuture.actionGet()).thenReturn(mockBulkResponse);

         when(mockBulkRequestBuilder.execute()).thenReturn(mockListenableActionFuture);

         Thread thread = new Thread(rabbitmqConsumer);
         thread.start();

         // wait for it to pick up 1 message
         with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                 .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));
         // wait for work to stop
         with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                 .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

         InOrder inOrder = inOrder(mockIndexingConnection, mockIndexingChannel, mockQueueingConsumer, mockClient, mockBulkRequestBuilder, mockBulkResponse);

         inOrder.verify(mockQueueingConsumer, times(1)).nextDelivery();
         inOrder.verify(mockClient, times(1)).prepareBulk();
         inOrder.verify(mockBulkRequestBuilder, times(1)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
         inOrder.verify(mockBulkRequestBuilder, times(1)).execute();
         inOrder.verify(mockBulkResponse).hasFailures();
         inOrder.verify(mockIndexingChannel).basicAck(10l, false);
         inOrder.verify(mockIndexingChannel).close(eq(0), any(String.class));
         inOrder.verify(mockIndexingConnection).close(eq(0), any(String.class));

         inOrder.verifyNoMoreInteractions();
         assertThat(message_counter.getCount(), equalTo(1));
     }


    /**
          * Mocks everything to do with Rabbitmq client.
          * Sends real message now we use the BulkRequestBuilder
          * bulk_size = 1  ==> so no attempts to concatenate multiple mesasages
          * Tests that:
          * - the mq channel is set up correctly
          * - 3 message are picked up
          * - message is added to bulkRequestBuilder
          * - bulkRequest is executed using sync method - only witness to failure is lost in ES log
          * - bulkresponse is queried for failures
          * - an ack message is sent
          * - the mq channel and connection closed
          */
         @Test
         public void testSyncBulkIndexingExecutionWithFailuresWithResponseDisabled() throws Exception {
             System.out.println("start testProcessGoodMessage");

             Map<String, Object> rabbitSettings = new HashMap<String, Object>();
             Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
             Map<String, Object> indexSettings = new HashMap<String, Object>();
             indexSettings.put("bulk_size", 1);
             indexSettings.put("ordered", true);

             RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                     rabbitSettings,
                     responseChannelSettings,
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
             final BulkResponse mockBulkResponse = mock(BulkResponse.class);
             when(mockBulkResponse.hasFailures()).thenReturn(true);

             ListenableActionFuture<BulkResponse> mockListenableActionFuture = mock(ListenableActionFuture.class);
             when(mockListenableActionFuture.actionGet()).thenReturn(mockBulkResponse);

             when(mockBulkRequestBuilder.execute()).thenReturn(mockListenableActionFuture);

             Thread thread = new Thread(rabbitmqConsumer);
             thread.start();

             // wait for it to pick up 1 message
             with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                     .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));
             // wait for work to stop
             with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                     .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

             InOrder inOrder = inOrder(mockIndexingConnection, mockIndexingChannel, mockQueueingConsumer, mockClient, mockBulkRequestBuilder, mockBulkResponse);

             inOrder.verify(mockQueueingConsumer, times(1)).nextDelivery();
             inOrder.verify(mockClient, times(1)).prepareBulk();
             inOrder.verify(mockBulkRequestBuilder, times(1)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
             inOrder.verify(mockBulkRequestBuilder, times(1)).execute();
             inOrder.verify(mockBulkResponse).hasFailures();
             inOrder.verify(mockIndexingChannel).basicAck(10l, false);
             inOrder.verify(mockIndexingChannel).close(eq(0), any(String.class));
             inOrder.verify(mockIndexingConnection).close(eq(0), any(String.class));

             inOrder.verifyNoMoreInteractions();
             assertThat(message_counter.getCount(), equalTo(1));
         }



    /**
             * Mocks everything to do with Rabbitmq client.
             * Sends real message now we use the BulkRequestBuilder
             * bulk_size = 1  ==> so no attempts to concatenate multiple mesasages
             * Tests that:
             * - the mq channel is set up correctly
             * - 3 message are picked up
             * - message is added to bulkRequestBuilder
             * - bulkRequest is executed using sync method - only witness to failure is lost in ES log
             * - bulkresponse is queried for failures
             * - an ack message is sent
             * - the mq channel and connection closed
             */
            @Test
            public void testSyncBulkIndexingExecutionThrowsExceptionWithResponseDisabled() throws Exception {
                System.out.println("start testProcessGoodMessage");

                Map<String, Object> rabbitSettings = new HashMap<String, Object>();
                Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
                Map<String, Object> indexSettings = new HashMap<String, Object>();
                indexSettings.put("bulk_size", 1);
                indexSettings.put("ordered", true);

                RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                        rabbitSettings,
                        responseChannelSettings,
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
                final BulkResponse mockBulkResponse = mock(BulkResponse.class);

                ListenableActionFuture<BulkResponse> mockListenableActionFuture = mock(ListenableActionFuture.class);
                when(mockListenableActionFuture.actionGet()).thenReturn(mockBulkResponse);
                when(mockBulkRequestBuilder.execute()).thenThrow(new RuntimeException("error: abc"));

                Thread thread = new Thread(rabbitmqConsumer);
                thread.start();

                // wait for it to pick up 1 message
                with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                        .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));
                // wait for work to stop
                with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                        .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

                InOrder inOrder = inOrder(mockIndexingConnection, mockIndexingChannel, mockQueueingConsumer, mockClient, mockBulkRequestBuilder, mockBulkResponse);

                inOrder.verify(mockQueueingConsumer, times(1)).nextDelivery();
                inOrder.verify(mockClient, times(1)).prepareBulk();
                inOrder.verify(mockBulkRequestBuilder, times(1)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
                inOrder.verify(mockBulkRequestBuilder, times(1)).execute();
                inOrder.verify(mockIndexingChannel).close(eq(0), any(String.class));
                inOrder.verify(mockIndexingConnection).close(eq(0), any(String.class));

                inOrder.verifyNoMoreInteractions();
                assertThat(message_counter.getCount(), equalTo(1));
            }



    /**
     * Mocks everything to do with Rabbitmq client.
     * Sends real message now we use the BulkRequestBuilder
     * bulk_size = 1  ==> so no attempts to concatenate multiple mesasages
     * Response messages enabled
     * Tests that:
     * - 1 message is picked up
     * - message is added to bulkRequestBuilder
     * - bulkRequest is executed using async method
     * - bulkresponse is queried for failures
     * - a response message is sent
     * - an ack message is sent
     * - the mq channel and connection closed
     */
    @Test
    public void testAsyncBulkIndexingExecutionWithResponseEnabled() throws Exception {
        System.out.println("start testProcessGoodMessage");

        Map<String, Object> rabbitSettings = new HashMap<String, Object>();
        Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
        responseChannelSettings.put("response_messages_enabled", true);
        responseChannelSettings.put("host", "host1");
        Map<String, Object> indexSettings = new HashMap<String, Object>();
        indexSettings.put("bulk_size", 1);

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                responseChannelSettings,
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
        final BulkResponse mockBulkResponse = mock(BulkResponse.class);
        when(mockBulkResponse.hasFailures()).thenReturn(false);

        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                ActionListener<BulkResponse> actionListener = (ActionListener<BulkResponse>) invocation.getArguments()[0];
                actionListener.onResponse(mockBulkResponse);
                return null;
            }
        }).when(mockBulkRequestBuilder).execute((ActionListener<BulkResponse>) any(ActionListener.class));

        Thread thread = new Thread(rabbitmqConsumer);
        thread.start();

        // wait for it to pick up 1 message
        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));
        // wait for work to stop
        with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

        InOrder inOrder = inOrder(mockIndexingConnection, mockIndexingChannel, mockResponseConnection, mockResponseChannel, mockQueueingConsumer, mockClient, mockBulkRequestBuilder, mockBulkResponse);

        inOrder.verify(mockQueueingConsumer, times(1)).nextDelivery();
        inOrder.verify(mockClient, times(1)).prepareBulk();
        inOrder.verify(mockBulkRequestBuilder, times(1)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
        inOrder.verify(mockBulkRequestBuilder, times(1)).execute((ActionListener<BulkResponse>) any(ActionListener.class));
        inOrder.verify(mockBulkResponse, atLeastOnce()).hasFailures();
        inOrder.verify(mockResponseChannel).basicPublish(eq(configHolder.getResponseExchange()),
                eq(configHolder.getResponseRoutingKey()),
                any(AMQP.BasicProperties.class),
                eq("{\"faultInfo\":null,\"bulkRequest\":\"{}\",\"success\":true}".getBytes()));

        inOrder.verify(mockIndexingChannel).basicAck(10l, false);

        inOrder.verify(mockIndexingChannel).close(eq(0), any(String.class));
        inOrder.verify(mockIndexingConnection).close(eq(0), any(String.class));
        inOrder.verify(mockResponseChannel).close(eq(0), any(String.class));
        inOrder.verify(mockResponseConnection).close(eq(0), any(String.class));

        inOrder.verifyNoMoreInteractions();
        assertThat(message_counter.getCount(), equalTo(1));
    }

    /**
         * Mocks everything to do with Rabbitmq client.
         * Sends real message now we use the BulkRequestBuilder
         * bulk_size = 1  ==> so no attempts to concatenate multiple mesasages
         * Response messages enabled
         * Tests that:
         * - 1 message is picked up
         * - message is added to bulkRequestBuilder
         * - bulkRequest is executed using async method
         * - bulkresponse is queried for failures - response true.
         * - a response message is sent
         * - an ack message is sent
         * - the mq channel and connection closed
         */
        @Test
        public void testAsyncBulkIndexingExecutionWithFailuresWithResponseEnabled() throws Exception {
            System.out.println("start testProcessGoodMessage");

            Map<String, Object> rabbitSettings = new HashMap<String, Object>();
            Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
            responseChannelSettings.put("response_messages_enabled", true);
            responseChannelSettings.put("host", "host1");
            Map<String, Object> indexSettings = new HashMap<String, Object>();
            indexSettings.put("bulk_size", 1);

            RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                    rabbitSettings,
                    responseChannelSettings,
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
            final BulkResponse mockBulkResponse = mock(BulkResponse.class);
            when(mockBulkResponse.hasFailures()).thenReturn(true);
            when(mockBulkResponse.buildFailureMessage()).thenReturn("xx:yy - zz");


            doAnswer(new Answer() {
                public Object answer(InvocationOnMock invocation) {
                    Object[] args = invocation.getArguments();
                    ActionListener<BulkResponse> actionListener = (ActionListener<BulkResponse>) invocation.getArguments()[0];
                    actionListener.onResponse(mockBulkResponse);
                    return null;
                }
            }).when(mockBulkRequestBuilder).execute((ActionListener<BulkResponse>) any(ActionListener.class));

            Thread thread = new Thread(rabbitmqConsumer);
            thread.start();

            // wait for it to pick up 1 message
            with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                    .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));
            // wait for work to stop
            with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                    .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

            InOrder inOrder = inOrder(mockIndexingConnection, mockIndexingChannel, mockResponseConnection, mockResponseChannel, mockQueueingConsumer, mockClient, mockBulkRequestBuilder, mockBulkResponse);

            inOrder.verify(mockQueueingConsumer, times(1)).nextDelivery();
            inOrder.verify(mockClient, times(1)).prepareBulk();
            inOrder.verify(mockBulkRequestBuilder, times(1)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
            inOrder.verify(mockBulkRequestBuilder, times(1)).execute((ActionListener<BulkResponse>) any(ActionListener.class));
            inOrder.verify(mockBulkResponse).hasFailures();
            inOrder.verify(mockResponseChannel).basicPublish(eq(configHolder.getResponseExchange()),
                    eq(configHolder.getResponseRoutingKey()),
                    any(AMQP.BasicProperties.class),
                    eq("{\"faultInfo\":{\"errorMessage\":\"xx:yy - zz\",\"errorName\":\"BulkResponse.Failures\"},\"bulkRequest\":\"{}\",\"success\":false}".getBytes()));

            inOrder.verify(mockIndexingChannel).basicAck(10l, false);

            inOrder.verify(mockIndexingChannel).close(eq(0), any(String.class));
            inOrder.verify(mockIndexingConnection).close(eq(0), any(String.class));
            inOrder.verify(mockResponseChannel).close(eq(0), any(String.class));
            inOrder.verify(mockResponseConnection).close(eq(0), any(String.class));

            inOrder.verifyNoMoreInteractions();
            assertThat(message_counter.getCount(), equalTo(1));
        }


    /**
           * Mocks everything to do with Rabbitmq client.
           * Sends real message now we use the BulkRequestBuilder
           * bulk_size = 1  ==> so no attempts to concatenate multiple mesasages
           * Response messages enabled
           * Tests that:
           * - 1 message is picked up
           * - message is added to bulkRequestBuilder
           * - bulkRequest is executed using async method
           * - bulkresponse is queried for failures - response true.
           * - a response message is sent
           * - an ack message is sent
           * - the mq channel and connection closed
           */
          @Test
          public void testAsyncBulkIndexingExecutionWithTotalFailureWithResponseEnabled() throws Exception {
              System.out.println("start testProcessGoodMessage");

              Map<String, Object> rabbitSettings = new HashMap<String, Object>();
              Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
              responseChannelSettings.put("response_messages_enabled", true);
              responseChannelSettings.put("host", "host1");
              Map<String, Object> indexSettings = new HashMap<String, Object>();
              indexSettings.put("bulk_size", 1);

              RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                      rabbitSettings,
                      responseChannelSettings,
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
              final BulkResponse mockBulkResponse = mock(BulkResponse.class);
              when(mockBulkResponse.buildFailureMessage()).thenReturn("xx:yy - zz");

              doAnswer(new Answer() {
                  public Object answer(InvocationOnMock invocation) {
                      Object[] args = invocation.getArguments();
                      ActionListener<BulkResponse> actionListener = (ActionListener<BulkResponse>) invocation.getArguments()[0];
                      actionListener.onFailure(new Throwable("error: abc"));
                      return null;
                  }
              }).when(mockBulkRequestBuilder).execute((ActionListener<BulkResponse>) any(ActionListener.class));

              Thread thread = new Thread(rabbitmqConsumer);
              thread.start();

              // wait for it to pick up 1 message
              with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                      .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));
              // wait for work to stop
              with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                      .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

              InOrder inOrder = inOrder(mockIndexingConnection, mockIndexingChannel, mockResponseConnection, mockResponseChannel, mockQueueingConsumer, mockClient, mockBulkRequestBuilder, mockBulkResponse);

              inOrder.verify(mockQueueingConsumer, times(1)).nextDelivery();
              inOrder.verify(mockClient, times(1)).prepareBulk();
              inOrder.verify(mockBulkRequestBuilder, times(1)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
              inOrder.verify(mockBulkRequestBuilder, times(1)).execute((ActionListener<BulkResponse>) any(ActionListener.class));
              inOrder.verify(mockResponseChannel).basicPublish(eq(configHolder.getResponseExchange()),
                      eq(configHolder.getResponseRoutingKey()),
                      any(AMQP.BasicProperties.class),
                      eq("{\"faultInfo\":{\"errorMessage\":\"error: abc\",\"errorName\":\"java.lang.Throwable\"},\"bulkRequest\":\"{}\",\"success\":false}".getBytes()));


              inOrder.verify(mockIndexingChannel).close(eq(0), any(String.class));
              inOrder.verify(mockIndexingConnection).close(eq(0), any(String.class));
              inOrder.verify(mockResponseChannel).close(eq(0), any(String.class));
              inOrder.verify(mockResponseConnection).close(eq(0), any(String.class));

              inOrder.verifyNoMoreInteractions();
              assertThat(message_counter.getCount(), equalTo(1));
          }

    /**
          * Mocks everything to do with Rabbitmq client.
          * Sends real message now we use the BulkRequestBuilder
          * bulk_size = 1  ==> so no attempts to concatenate multiple mesasages
          * Tests that:
          * - the mq channel is set up correctly
          * - 3 message are picked up
          * - message is added to bulkRequestBuilder
          * - bulkRequest is executed using sync method
          * - bulkresponse is queried for failures
          * - a response message is sent
          * - an ack message is sent
          * - the mq channel and connection closed
          */
         @Test
         public void testSyncBulkIndexingExecutionWithResponseEnabled() throws Exception {
             System.out.println("start testProcessGoodMessage");

             Map<String, Object> rabbitSettings = new HashMap<String, Object>();
             Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
             responseChannelSettings.put("response_messages_enabled", true);
                     responseChannelSettings.put("host", "host1");
             Map<String, Object> indexSettings = new HashMap<String, Object>();
             indexSettings.put("bulk_size", 1);
             indexSettings.put("ordered", true);

             RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                     rabbitSettings,
                     responseChannelSettings,
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
             final BulkResponse mockBulkResponse = mock(BulkResponse.class);
             when(mockBulkResponse.hasFailures()).thenReturn(false);

             ListenableActionFuture<BulkResponse> mockListenableActionFuture = mock(ListenableActionFuture.class);
             when(mockListenableActionFuture.actionGet()).thenReturn(mockBulkResponse);

             when(mockBulkRequestBuilder.execute()).thenReturn(mockListenableActionFuture);

             Thread thread = new Thread(rabbitmqConsumer);
             thread.start();

             // wait for it to pick up 1 message
             with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                     .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));
             // wait for work to stop
             with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                     .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

             InOrder inOrder = inOrder(mockIndexingConnection, mockIndexingChannel, mockResponseConnection, mockResponseChannel, mockQueueingConsumer, mockClient, mockBulkRequestBuilder);

             inOrder.verify(mockQueueingConsumer, times(1)).nextDelivery();
             inOrder.verify(mockClient, times(1)).prepareBulk();
             inOrder.verify(mockBulkRequestBuilder, times(1)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
             inOrder.verify(mockBulkRequestBuilder, times(1)).execute();
             inOrder.verify(mockResponseChannel).basicPublish(eq(configHolder.getResponseExchange()),
                             eq(configHolder.getResponseRoutingKey()),
                             any(AMQP.BasicProperties.class),
                             eq("{\"faultInfo\":null,\"bulkRequest\":\"{}\",\"success\":true}".getBytes()));
             inOrder.verify(mockIndexingChannel).basicAck(10l, false);
             inOrder.verify(mockIndexingChannel).close(eq(0), any(String.class));
             inOrder.verify(mockIndexingConnection).close(eq(0), any(String.class));
             inOrder.verify(mockResponseChannel).close(eq(0), any(String.class));
             inOrder.verify(mockResponseConnection).close(eq(0), any(String.class));

             inOrder.verifyNoMoreInteractions();
             assertThat(message_counter.getCount(), equalTo(1));
         }

    /**
           * Mocks everything to do with Rabbitmq client.
           * Sends real message now we use the BulkRequestBuilder
           * bulk_size = 1  ==> so no attempts to concatenate multiple mesasages
           * Tests that:
           * - the mq channel is set up correctly
           * - 3 message are picked up
           * - message is added to bulkRequestBuilder
           * - bulkRequest is executed using sync method
           * - bulkresponse is queried for failures - response true
           * - a response message is sent
           * - an ack message is sent
           * - the mq channel and connection closed
           */
          @Test
          public void testSyncBulkIndexingExecutionThrowsExceptionWithResponseEnabled() throws Exception {
              System.out.println("start testProcessGoodMessage");

              Map<String, Object> rabbitSettings = new HashMap<String, Object>();
              Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
              responseChannelSettings.put("response_messages_enabled", true);
                      responseChannelSettings.put("host", "host1");
              Map<String, Object> indexSettings = new HashMap<String, Object>();
              indexSettings.put("bulk_size", 1);
              indexSettings.put("ordered", true);

              RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                      rabbitSettings,
                      responseChannelSettings,
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
              final BulkResponse mockBulkResponse = mock(BulkResponse.class);

              ListenableActionFuture<BulkResponse> mockListenableActionFuture = mock(ListenableActionFuture.class);
              when(mockListenableActionFuture.actionGet()).thenReturn(mockBulkResponse);

              when(mockBulkRequestBuilder.execute()).thenThrow(new RuntimeException("error: abc"));

              Thread thread = new Thread(rabbitmqConsumer);
              thread.start();

              // wait for it to pick up 1 message
              with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until 1 messages processed")
                      .atMost(20, TimeUnit.SECONDS).untilCall(to(message_counter).getCount(), equalTo(1));
              // wait for work to stop
              with().pollInterval(20, TimeUnit.MILLISECONDS).and().pollDelay(20, TimeUnit.MILLISECONDS).await("until no longer running")
                      .atMost(20, TimeUnit.SECONDS).untilCall(to(rabbitmqConsumer).isRunning(), equalTo(false));

              InOrder inOrder = inOrder(mockIndexingConnection, mockIndexingChannel, mockResponseConnection, mockResponseChannel, mockQueueingConsumer, mockClient, mockBulkRequestBuilder);

              inOrder.verify(mockQueueingConsumer, times(1)).nextDelivery();
              inOrder.verify(mockClient, times(1)).prepareBulk();
              inOrder.verify(mockBulkRequestBuilder, times(1)).add(goodMessage.getBody(), 0, goodMessage.getBody().length, false);
              inOrder.verify(mockBulkRequestBuilder, times(1)).execute();
              inOrder.verify(mockResponseChannel).basicPublish(eq(configHolder.getResponseExchange()),
                              eq(configHolder.getResponseRoutingKey()),
                              any(AMQP.BasicProperties.class),
                              eq("{\"faultInfo\":{\"errorMessage\":\"error: abc\",\"errorName\":\"java.lang.RuntimeException\"},\"bulkRequest\":\"{}\",\"success\":false}".getBytes()));
              inOrder.verify(mockIndexingChannel).close(eq(0), any(String.class));
              inOrder.verify(mockIndexingConnection).close(eq(0), any(String.class));
              inOrder.verify(mockResponseChannel).close(eq(0), any(String.class));
              inOrder.verify(mockResponseConnection).close(eq(0), any(String.class));

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
    public void testProcess3GoodMessagesOrderedTrueResponseDisabled() throws Exception {
        System.out.println("start testProcess3GoodMessagesOrderedTrue");
        //  public RabbitmqRiverConfigHolder(Map<String, Object> rabbitSettings, Map<String, Object> indexSettings)
        Map<String, Object> rabbitSettings = new HashMap<String, Object>();
        Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
        Map<String, Object> indexSettings = new HashMap<String, Object>();
        indexSettings.put("bulk_size", 1);
        // indexSettings.put("bulk_timeout","");
        indexSettings.put("ordered", true);

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                responseChannelSettings,
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
        verify(mockIndexingChannel, times(3)).basicAck(10l, false);

        verify(mockIndexingChannel).close(eq(0), any(String.class));
        verify(mockIndexingConnection).close(eq(0), any(String.class));

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
    public void testProcess3GoodMessagesOrderedFalseResponseDisabled() throws Exception {
        System.out.println("start testProcess3GoodMessagesOrderedFalse");
        Map<String, Object> rabbitSettings = new HashMap<String, Object>();
        Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
        Map<String, Object> indexSettings = new HashMap<String, Object>();
        indexSettings.put("bulk_size", 1);

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                responseChannelSettings,
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
        // verify(mockIndexingChannel, times(3)).basicAck(10l, false);

        verify(mockIndexingChannel).close(eq(0), any(String.class));
        verify(mockIndexingConnection).close(eq(0), any(String.class));

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
    public void testProcess3GoodMessagesInSingleBulkExecutionOrderedTrueResponseDisabled() throws Exception {
        System.out.println("start testProcess3GoodMessagesInSingleBulkExecutionOrderedTrue");

        Map<String, Object> rabbitSettings = new HashMap<String, Object>();
        Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
        Map<String, Object> indexSettings = new HashMap<String, Object>();
        indexSettings.put("bulk_size", 10);
        indexSettings.put("bulk_timeout", "1000");
        indexSettings.put("ordered", true);

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                responseChannelSettings,
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
        verify(mockIndexingChannel, times(3)).basicAck(10l, false);

        verify(mockIndexingChannel).close(eq(0), any(String.class));
        verify(mockIndexingConnection).close(eq(0), any(String.class));

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
    public void testProcess3GoodMessagesInSingleBulkExecutionOrderedFalseResponseDisabled() throws Exception {
        System.out.println("start testProcess3GoodMessagesInSingleBulkExecutionOrderedFalse");

        Map<String, Object> rabbitSettings = new HashMap<String, Object>();
        Map<String, Object> responseChannelSettings = new HashMap<String, Object>();
        Map<String, Object> indexSettings = new HashMap<String, Object>();
        indexSettings.put("bulk_size", 10);
        indexSettings.put("bulk_timeout", "1000");
        indexSettings.put("ordered", false);

        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                rabbitSettings,
                responseChannelSettings,
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
        //verify(mockIndexingChannel, times(3)).basicAck(10l, false);

        verify(mockIndexingChannel).close(eq(0), any(String.class));
        verify(mockIndexingConnection).close(eq(0), any(String.class));

        // we're not using the sync method
        verify(mockBulkRequestBuilder, never()).execute();
    }


}
