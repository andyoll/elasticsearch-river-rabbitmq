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
                (Map<String, Object>) riverSettingsMap.get("index"));

        Address[] defaultRabbitAddresses = new Address[]{new Address("host1", 1234)};
        //todo add multiple addresses
        assertThat(configHolder.getRabbitAddresses(), equalTo(defaultRabbitAddresses));
        assertThat(configHolder.getRabbitVhost(), equalTo("vhost1"));
        assertThat(configHolder.getRabbitUser(), equalTo("user1"));
        assertThat(configHolder.getRabbitPassword(), equalTo("pass1"));
        assertThat(configHolder.getRabbitExchange(), equalTo("exchange_name"));
        assertThat(configHolder.getRabbitExchangeType(), equalTo("exchange_type"));
        assertThat(configHolder.isRabbitExchangeDurable(), equalTo(false));
        assertThat(configHolder.getRabbitQueue(), equalTo("queue_name"));
        assertThat(configHolder.isRabbitQueueAutoDelete(), equalTo(true));
        assertThat(configHolder.isRabbitQueueDurable(), equalTo(false));
        //TODO add some args
        assertThat(configHolder.getRabbitQueueArgs(), equalTo(null));
        assertThat(configHolder.getRabbitRoutingKey(), equalTo("routing_key"));
        assertThat(configHolder.getBulkSize(), equalTo(200));
        assertThat(configHolder.getBulkTimeout(), equalTo(new TimeValue(20)));
        assertThat(configHolder.isOrdered(), equalTo(true));
    }

    @Test
    public void RabbitmqRiverConfigHolderWithDefaultConfigTest() throws IOException, InterruptedException {
        RabbitmqRiverConfigHolder configHolder = new RabbitmqRiverConfigHolder(
                new HashMap<String, Object>(),
                new HashMap<String, Object>());

        Address[] defaultRabbitAddresses = new Address[]{new Address("localhost", AMQP.PROTOCOL.PORT)};
        assertThat(configHolder.getRabbitAddresses(), equalTo(defaultRabbitAddresses));
        assertThat(configHolder.getRabbitVhost(), equalTo("/"));
        assertThat(configHolder.getRabbitUser(), equalTo("guest"));
        assertThat(configHolder.getRabbitPassword(), equalTo("guest"));
        assertThat(configHolder.getRabbitExchange(), equalTo("elasticsearch"));
        assertThat(configHolder.getRabbitExchangeType(), equalTo("direct"));
        assertThat(configHolder.isRabbitExchangeDurable(), equalTo(true));
        assertThat(configHolder.getRabbitQueue(), equalTo("elasticsearch"));
        assertThat(configHolder.isRabbitQueueAutoDelete(), equalTo(false));
        assertThat(configHolder.isRabbitQueueDurable(), equalTo(true));
        assertThat(configHolder.getRabbitQueueArgs(), equalTo(null));
        assertThat(configHolder.getRabbitRoutingKey(), equalTo("elasticsearch"));
        assertThat(configHolder.getBulkSize(), equalTo(100));
        assertThat(configHolder.getBulkTimeout(), equalTo(new TimeValue(10)));
        assertThat(configHolder.isOrdered(), equalTo(false));
    }

}
