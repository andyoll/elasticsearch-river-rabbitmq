package org.elasticsearch.river.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
class RabbitmqRiverConfigHolder {

    // Rabbitmq Settings
    private final Address[] rabbitAddresses;
    private final String rabbitUser;
    private final String rabbitPassword;
    private final String rabbitVhost;
    private final String rabbitQueue;
    private final String rabbitExchange;
    private final String rabbitExchangeType;
    private final String rabbitRoutingKey;
    private final boolean rabbitExchangeDurable;
    private final boolean rabbitQueueDurable;
    private final boolean rabbitQueueAutoDelete;
    private final Map<String, Object> rabbitQueueArgs; //extra arguments passed to queue for creation (ha settings for example)

    // Indexing settings
    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final boolean ordered;

    @SuppressWarnings("unchecked")
    public RabbitmqRiverConfigHolder(Map<String, Object> rabbitSettings, Map<String, Object> indexSettings) {
        if (rabbitSettings.containsKey("addresses")) {
            List<Address> addresses = new ArrayList<Address>();
            for (Map<String, Object> address : (List<Map<String, Object>>) rabbitSettings.get("addresses")) {
                addresses.add(new Address(XContentMapValues.nodeStringValue(address.get("host"), "localhost"),
                        XContentMapValues.nodeIntegerValue(address.get("port"), AMQP.PROTOCOL.PORT)));
            }
            rabbitAddresses = addresses.toArray(new Address[addresses.size()]);
        } else {
            String rabbitHost = XContentMapValues.nodeStringValue(rabbitSettings.get("host"), "localhost");
            int rabbitPort = XContentMapValues.nodeIntegerValue(rabbitSettings.get("port"), AMQP.PROTOCOL.PORT);
            rabbitAddresses = new Address[]{new Address(rabbitHost, rabbitPort)};
        }

        rabbitUser = XContentMapValues.nodeStringValue(rabbitSettings.get("user"), "guest");
        rabbitPassword = XContentMapValues.nodeStringValue(rabbitSettings.get("pass"), "guest");
        rabbitVhost = XContentMapValues.nodeStringValue(rabbitSettings.get("vhost"), "/");

        rabbitQueue = XContentMapValues.nodeStringValue(rabbitSettings.get("queue"), "elasticsearch");
        rabbitExchange = XContentMapValues.nodeStringValue(rabbitSettings.get("exchange"), "elasticsearch");
        rabbitExchangeType = XContentMapValues.nodeStringValue(rabbitSettings.get("exchange_type"), "direct");
        rabbitRoutingKey = XContentMapValues.nodeStringValue(rabbitSettings.get("routing_key"), "elasticsearch");
        rabbitExchangeDurable = XContentMapValues.nodeBooleanValue(rabbitSettings.get("exchange_durable"), true);
        rabbitQueueDurable = XContentMapValues.nodeBooleanValue(rabbitSettings.get("queue_durable"), true);
        rabbitQueueAutoDelete = XContentMapValues.nodeBooleanValue(rabbitSettings.get("queue_auto_delete"), false);

        if (rabbitSettings.containsKey("args")) {
            Map<String, Object> argsMap = (Map<String, Object>) rabbitSettings.get("args");
            rabbitQueueArgs = Collections.unmodifiableMap(argsMap);
        } else {
            rabbitQueueArgs = null;
        }

        bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
        if (indexSettings.containsKey("bulk_timeout")) {
            bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10));
        } else {
            bulkTimeout = TimeValue.timeValueMillis(10);
        }
        ordered = XContentMapValues.nodeBooleanValue(indexSettings.get("ordered"), false);
    }


    public Address[] getRabbitAddresses() {
        return rabbitAddresses;
    }

    public String getRabbitUser() {
        return rabbitUser;
    }

    public String getRabbitPassword() {
        return rabbitPassword;
    }

    public String getRabbitVhost() {
        return rabbitVhost;
    }

    public String getRabbitQueue() {
        return rabbitQueue;
    }

    public String getRabbitExchange() {
        return rabbitExchange;
    }

    public String getRabbitExchangeType() {
        return rabbitExchangeType;
    }

    public String getRabbitRoutingKey() {
        return rabbitRoutingKey;
    }

    public boolean isRabbitExchangeDurable() {
        return rabbitExchangeDurable;
    }

    public boolean isRabbitQueueDurable() {
        return rabbitQueueDurable;
    }

    public boolean isRabbitQueueAutoDelete() {
        return rabbitQueueAutoDelete;
    }

    public Map<String, Object> getRabbitQueueArgs() {
        return rabbitQueueArgs;
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public TimeValue getBulkTimeout() {
        return bulkTimeout;
    }

    public boolean isOrdered() {
        return ordered;
    }

}
