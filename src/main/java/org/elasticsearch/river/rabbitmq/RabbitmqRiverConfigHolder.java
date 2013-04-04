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


    // Index q settings
    private final Address[] indexAddresses;
    private final String indexUser;
    private final String indexPassword;
    private final String indexVhost;
    private final String indexQueue;
    private final String indexExchange;
    private final String indexExchangeType;
    private final String indexRoutingKey;
    private final boolean indexExchangeDurable;
    private final boolean indexQueueDurable;
    private final boolean indexQueueAutoDelete;
    private final Map<String, Object> indexQueueArgs; //extra arguments passed to queue for creation (ha settings for example)

    // Resonse q settings
    private boolean responseMessagesEnabled;
    private final String responseAppId;
    private final Address[] responseAddresses;
    private final String responseUser;
    private final String responsePassword;
    private final String responseVhost;
    private final String responseQueue;
    private final String responseExchange;
    private final String responseExchangeType;
    private final String responseRoutingKey;
    private final boolean responseExchangeDurable;
    private final boolean responseQueueDurable;
    private final boolean responseQueueAutoDelete;
    private final Map<String, Object> responseQueueArgs; //extra arguments passed to queue for creation (ha settings for example)


    // Indexing settings
    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final boolean ordered;

    @SuppressWarnings("unchecked")
    public RabbitmqRiverConfigHolder(Map<String, Object> rabbitSettings,
                                     Map<String, Object> responseChannelSettings,
                                     Map<String, Object> indexSettings) {
        // index q settings
        if (rabbitSettings.containsKey("addresses")) {
            List<Address> addresses = new ArrayList<Address>();
            for (Map<String, Object> address : (List<Map<String, Object>>) rabbitSettings.get("addresses")) {
                addresses.add(new Address(XContentMapValues.nodeStringValue(address.get("host"), "localhost"),
                        XContentMapValues.nodeIntegerValue(address.get("port"), AMQP.PROTOCOL.PORT)));
            }
            indexAddresses = addresses.toArray(new Address[addresses.size()]);
        } else {
            String rabbitHost = XContentMapValues.nodeStringValue(rabbitSettings.get("host"), "localhost");
            int rabbitPort = XContentMapValues.nodeIntegerValue(rabbitSettings.get("port"), AMQP.PROTOCOL.PORT);
            indexAddresses = new Address[]{new Address(rabbitHost, rabbitPort)};
        }

        indexUser = XContentMapValues.nodeStringValue(rabbitSettings.get("user"), "guest");
        indexPassword = XContentMapValues.nodeStringValue(rabbitSettings.get("pass"), "guest");
        indexVhost = XContentMapValues.nodeStringValue(rabbitSettings.get("vhost"), "/");

        indexQueue = XContentMapValues.nodeStringValue(rabbitSettings.get("queue"), "elasticsearch");
        indexExchange = XContentMapValues.nodeStringValue(rabbitSettings.get("exchange"), "elasticsearch");
        indexExchangeType = XContentMapValues.nodeStringValue(rabbitSettings.get("exchange_type"), "direct");
        indexRoutingKey = XContentMapValues.nodeStringValue(rabbitSettings.get("routing_key"), "elasticsearch");
        indexExchangeDurable = XContentMapValues.nodeBooleanValue(rabbitSettings.get("exchange_durable"), true);
        indexQueueDurable = XContentMapValues.nodeBooleanValue(rabbitSettings.get("queue_durable"), true);
        indexQueueAutoDelete = XContentMapValues.nodeBooleanValue(rabbitSettings.get("queue_auto_delete"), false);

        if (rabbitSettings.containsKey("args")) {
            Map<String, Object> argsMap = (Map<String, Object>) rabbitSettings.get("args");
            indexQueueArgs = Collections.unmodifiableMap(argsMap);
        } else {
            indexQueueArgs = null;
        }

        // response channel settings:
        responseMessagesEnabled = XContentMapValues.nodeBooleanValue(responseChannelSettings.get("response_messages_enabled"), false);
        responseAppId = XContentMapValues.nodeStringValue(responseChannelSettings.get("response_app_id"), "elasticsearch");

        if (responseChannelSettings.containsKey("addresses")) {
            List<Address> addresses = new ArrayList<Address>();
            for (Map<String, Object> address : (List<Map<String, Object>>) responseChannelSettings.get("addresses")) {
                addresses.add(new Address(XContentMapValues.nodeStringValue(address.get("host"), "localhost"),
                        XContentMapValues.nodeIntegerValue(address.get("port"), AMQP.PROTOCOL.PORT)));
            }
            responseAddresses = addresses.toArray(new Address[addresses.size()]);
        } else {
            String rabbitHost = XContentMapValues.nodeStringValue(responseChannelSettings.get("host"), "localhost");
            int rabbitPort = XContentMapValues.nodeIntegerValue(responseChannelSettings.get("port"), AMQP.PROTOCOL.PORT);
            responseAddresses = new Address[]{new Address(rabbitHost, rabbitPort)};
        }

        responseUser = XContentMapValues.nodeStringValue(responseChannelSettings.get("user"), "guest");
        responsePassword = XContentMapValues.nodeStringValue(responseChannelSettings.get("pass"), "guest");
        responseVhost = XContentMapValues.nodeStringValue(responseChannelSettings.get("vhost"), "/");

        responseQueue = XContentMapValues.nodeStringValue(responseChannelSettings.get("queue"), "elasticsearch_response");
        responseExchange = XContentMapValues.nodeStringValue(responseChannelSettings.get("exchange"), "elasticsearch");
        responseExchangeType = XContentMapValues.nodeStringValue(responseChannelSettings.get("exchange_type"), "direct");
        responseRoutingKey = XContentMapValues.nodeStringValue(responseChannelSettings.get("routing_key"), "elasticsearch");
        responseExchangeDurable = XContentMapValues.nodeBooleanValue(responseChannelSettings.get("exchange_durable"), true);
        responseQueueDurable = XContentMapValues.nodeBooleanValue(responseChannelSettings.get("queue_durable"), true);
        responseQueueAutoDelete = XContentMapValues.nodeBooleanValue(responseChannelSettings.get("queue_auto_delete"), false);

        if (rabbitSettings.containsKey("args")) {
            Map<String, Object> argsMap = (Map<String, Object>) responseChannelSettings.get("args");
            responseQueueArgs = Collections.unmodifiableMap(argsMap);
        } else {
            responseQueueArgs = null;
        }

        bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
        if (indexSettings.containsKey("bulk_timeout")) {
            bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10));
        } else {
            bulkTimeout = TimeValue.timeValueMillis(10);
        }
        ordered = XContentMapValues.nodeBooleanValue(indexSettings.get("ordered"), false);
    }


    public Address[] getIndexAddresses() {
        return indexAddresses;
    }

    public String getIndexUser() {
        return indexUser;
    }

    public String getIndexPassword() {
        return indexPassword;
    }

    public String getIndexVhost() {
        return indexVhost;
    }

    public String getIndexQueue() {
        return indexQueue;
    }

    public String getIndexExchange() {
        return indexExchange;
    }

    public String getIndexExchangeType() {
        return indexExchangeType;
    }

    public String getIndexRoutingKey() {
        return indexRoutingKey;
    }

    public boolean isIndexExchangeDurable() {
        return indexExchangeDurable;
    }

    public boolean isIndexQueueDurable() {
        return indexQueueDurable;
    }

    public boolean isIndexQueueAutoDelete() {
        return indexQueueAutoDelete;
    }

    public Map<String, Object> getIndexQueueArgs() {
        return indexQueueArgs;
    }


    public boolean isResponseMessagesEnabled() {
        return responseMessagesEnabled;
    }

    public String getResponseVhost() {
        return responseVhost;
    }

    public Address[] getResponseAddresses() {
        return responseAddresses;
    }

    public String getResponseUser() {
        return responseUser;
    }

    public String getResponsePassword() {
        return responsePassword;
    }

    public String getResponseQueue() {
        return responseQueue;
    }

    public String getResponseExchange() {
        return responseExchange;
    }

    public String getResponseExchangeType() {
        return responseExchangeType;
    }

    public String getResponseRoutingKey() {
        return responseRoutingKey;
    }

    public boolean isResponseExchangeDurable() {
        return responseExchangeDurable;
    }

    public boolean isResponseQueueDurable() {
        return responseQueueDurable;
    }

    public boolean isResponseQueueAutoDelete() {
        return responseQueueAutoDelete;
    }

    public Map<String, Object> getResponseQueueArgs() {
        return responseQueueArgs;
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

    public String getResponseAppId() {
        return responseAppId;
    }

}
