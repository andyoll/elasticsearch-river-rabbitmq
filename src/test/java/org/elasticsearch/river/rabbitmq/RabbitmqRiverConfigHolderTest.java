package org.elasticsearch.river.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import org.elasticsearch.common.unit.TimeValue;
import org.json.simple.JSONValue;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;


public class RabbitmqRiverConfigHolderTest {

    Map<String, Object> riverSettingsMap;

    @BeforeClass
    @SuppressWarnings("unchecked")
    public void oneTimeSetUp() {
        String jsonRiverConfig = "{\n" +
                "    \"type\" : \"rabbitmq\",\n" +
                "    \"rabbitmq\" : {\n" +
                "        \"host\" : \"host1\", \n" +
                "        \"port\" : 1234,\n" +
                "        \"user\" : \"user1\",\n" +
                "        \"pass\" : \"pass1\",\n" +
                "        \"vhost\" : \"vhost1\",\n" +
                "        \"queue\" : \"queue_name\",\n" +
                "        \"exchange\" : \"exchange_name\",\n" +
                "        \"routing_key\" : \"routing_key\",\n" +
                "        \"exchange_type\" : \"exchange_type\",\n" +
                "        \"exchange_durable\" : false,\n" +
                "        \"queue_durable\" : false,\n" +
                "        \"queue_auto_delete\" : true\n" +
                "    },\n" +
                "    \"responseChannel\" : {\n" +
                "        \"response_messages_enabled\" : true, \n" +
                "        \"response_app_id\" : \"cluster1\", \n" +
                "        \"host\" : \"host1\", \n" +
                "        \"port\" : 1234,\n" +
                "        \"user\" : \"user1\",\n" +
                "        \"pass\" : \"pass1\",\n" +
                "        \"vhost\" : \"vhost1\",\n" +
                "        \"queue\" : \"queue_name\",\n" +
                "        \"exchange\" : \"exchange_name\",\n" +
                "        \"routing_key\" : \"routing_key\",\n" +
                "        \"exchange_type\" : \"exchange_type\",\n" +
                "        \"exchange_durable\" : false,\n" +
                "        \"queue_durable\" : false,\n" +
                "        \"queue_auto_delete\" : true\n" +
                "    },\n" +
                "    \"index\" : {\n" +
                "        \"bulk_size\" : 200,\n" +
                "        \"bulk_timeout\" : \"20\",\n" +
                "        \"ordered\" : true\n" +
                "    }\n" +
                "}";

        riverSettingsMap = (Map<String, Object>) JSONValue.parse(jsonRiverConfig);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void RabbitmqRiverConfigHolderWithCustomConfigTest() throws IOException, InterruptedException {
        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                (Map<String, Object>) riverSettingsMap.get("rabbitmq"),
                (Map<String, Object>) riverSettingsMap.get("responseChannel"),
                (Map<String, Object>) riverSettingsMap.get("index"));

        Address[] defaultRabbitAddresses = new Address[]{new Address("host1", 1234)};

        // assert index channel data set ok
        //todo add multiple addresses
        assertThat(configHolder.getIndexAddresses(), equalTo(defaultRabbitAddresses));
        assertThat(configHolder.getIndexVhost(), equalTo("vhost1"));
        assertThat(configHolder.getIndexUser(), equalTo("user1"));
        assertThat(configHolder.getIndexPassword(), equalTo("pass1"));
        assertThat(configHolder.getIndexExchange(), equalTo("exchange_name"));
        assertThat(configHolder.getIndexExchangeType(), equalTo("exchange_type"));
        assertThat(configHolder.isIndexExchangeDurable(), equalTo(false));
        assertThat(configHolder.getIndexQueue(), equalTo("queue_name"));
        assertThat(configHolder.isIndexQueueAutoDelete(), equalTo(true));
        assertThat(configHolder.isIndexQueueDurable(), equalTo(false));
        //TODO add some args
        assertThat(configHolder.getIndexQueueArgs(), equalTo(null));
        assertThat(configHolder.getIndexRoutingKey(), equalTo("routing_key"));

        // assert response channel data set ok
        //todo add multiple addresses
        assertThat(configHolder.isResponseMessagesEnabled(), equalTo(true));
        assertThat(configHolder.getResponseAppId(), equalTo("cluster1"));
        assertThat(configHolder.getResponseAddresses(), equalTo(defaultRabbitAddresses));
        assertThat(configHolder.getResponseVhost(), equalTo("vhost1"));
        assertThat(configHolder.getResponseUser(), equalTo("user1"));
        assertThat(configHolder.getResponsePassword(), equalTo("pass1"));
        assertThat(configHolder.getResponseExchange(), equalTo("exchange_name"));
        assertThat(configHolder.getResponseExchangeType(), equalTo("exchange_type"));
        assertThat(configHolder.isResponseExchangeDurable(), equalTo(false));
        assertThat(configHolder.getResponseQueue(), equalTo("queue_name"));
        assertThat(configHolder.isResponseQueueAutoDelete(), equalTo(true));
        assertThat(configHolder.isResponseQueueDurable(), equalTo(false));
        //TODO add some args
        assertThat(configHolder.getResponseQueueArgs(), equalTo(null));
        assertThat(configHolder.getResponseRoutingKey(), equalTo("routing_key"));

        // assert ES indexing settings set ok
        assertThat(configHolder.getBulkSize(), equalTo(200));
        assertThat(configHolder.getBulkTimeout(), equalTo(new TimeValue(20)));
        assertThat(configHolder.isOrdered(), equalTo(true));
    }

    @Test
    public void RabbitmqRiverConfigHolderWithDefaultConfigTest() throws IOException, InterruptedException {
        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                new HashMap<String, Object>(),
                new HashMap<String, Object>(),
                new HashMap<String, Object>());

        Address[] defaultRabbitAddresses = new Address[]{new Address("localhost", AMQP.PROTOCOL.PORT)};
        assertThat(configHolder.getIndexAddresses(), equalTo(defaultRabbitAddresses));
        assertThat(configHolder.getIndexVhost(), equalTo("/"));
        assertThat(configHolder.getIndexUser(), equalTo("guest"));
        assertThat(configHolder.getIndexPassword(), equalTo("guest"));
        assertThat(configHolder.getIndexExchange(), equalTo("elasticsearch"));
        assertThat(configHolder.getIndexExchangeType(), equalTo("direct"));
        assertThat(configHolder.isIndexExchangeDurable(), equalTo(true));
        assertThat(configHolder.getIndexQueue(), equalTo("elasticsearch"));
        assertThat(configHolder.isIndexQueueAutoDelete(), equalTo(false));
        assertThat(configHolder.isIndexQueueDurable(), equalTo(true));
        assertThat(configHolder.getIndexQueueArgs(), equalTo(null));
        assertThat(configHolder.getIndexRoutingKey(), equalTo("elasticsearch"));
        assertThat(configHolder.getBulkSize(), equalTo(100));
        assertThat(configHolder.getBulkTimeout(), equalTo(new TimeValue(10)));
        assertThat(configHolder.isOrdered(), equalTo(false));

        // assert response channel data set ok
        assertThat(configHolder.isResponseMessagesEnabled(), equalTo(false));
        assertThat(configHolder.getResponseAppId(), equalTo("elasticsearch"));
        assertThat(configHolder.getResponseAddresses(), equalTo(defaultRabbitAddresses));
        assertThat(configHolder.getResponseVhost(), equalTo("/"));
        assertThat(configHolder.getResponseUser(), equalTo("guest"));
        assertThat(configHolder.getResponsePassword(), equalTo("guest"));
        assertThat(configHolder.getResponseExchange(), equalTo("elasticsearch"));
        assertThat(configHolder.getResponseExchangeType(), equalTo("direct"));
        assertThat(configHolder.isResponseExchangeDurable(), equalTo(true));
        assertThat(configHolder.getResponseQueue(), equalTo("elasticsearch_response"));
        assertThat(configHolder.isResponseQueueAutoDelete(), equalTo(false));
        assertThat(configHolder.isResponseQueueDurable(), equalTo(true));
        //TODO add some args
        assertThat(configHolder.getResponseQueueArgs(), equalTo(null));
        assertThat(configHolder.getResponseRoutingKey(), equalTo("elasticsearch"));

        // assert ES indexing settings set ok
        assertThat(configHolder.getBulkSize(), equalTo(100));
        assertThat(configHolder.getBulkTimeout(), equalTo(new TimeValue(10)));
        assertThat(configHolder.isOrdered(), equalTo(false));
    }

}
