RabbitMQ River Plugin for ElasticSearch
==================================

The RabbitMQ River plugin allows index bulk format messages into elasticsearch.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-river-rabbitmq/1.4.0`.

    --------------------------------------------------------
    | RabbitMQ Plugin | ElasticSearch    | RabbitMQ Client |
    --------------------------------------------------------
    | master          | 0.19 -> master   | 2.8.7           |
    --------------------------------------------------------
    | 1.4.0           | 0.19 -> master   | 2.8.4           |
    --------------------------------------------------------
    | 1.3.0           | 0.19 -> master   | 2.8.2           |
    --------------------------------------------------------
    | 1.2.0           | 0.19 -> master   | 2.8.1           |
    --------------------------------------------------------
    | 1.1.0           | 0.19 -> master   | 2.7.0           |
    --------------------------------------------------------
    | 1.0.0           | 0.18             | 2.7.0           |
    --------------------------------------------------------

RabbitMQ River allows to automatically index a [RabbitMQ](http://www.rabbitmq.com/) queue. The format of the messages follows the bulk api format:

	{ "index" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "this is a tweet" } }
	{ "delete" : { "_index" : "twitter", "_type" : "tweet", "_id" : "2" } }
	{ "create" : { "_index" : "twitter", "_type" : "tweet", "_id" : "1" } }
	{ "tweet" : { "text" : "another tweet" } }    

Creating the rabbitmq river is as simple as (all configuration parameters are provided, with default values):

	curl -XPUT 'localhost:9200/_river/my_river/_meta' -d '{
	    "type" : "rabbitmq",
	    "rabbitmq" : {
	        "host" : "localhost", 
	        "port" : 5672,
	        "user" : "guest",
	        "pass" : "guest",
	        "vhost" : "/",
	        "queue" : "elasticsearch",
	        "exchange" : "elasticsearch",
	        "routing_key" : "elasticsearch",
	        "exchange_type" : "direct",
	        "exchange_durable" : true,
	        "queue_durable" : true,
	        "queue_auto_delete" : false
	    },
	    "index" : {
	        "bulk_size" : 100,
	        "bulk_timeout" : "10ms",
	        "ordered" : false
	    }
	}'

Addresses(host-port pairs) also available. it is useful to taking advantage rabbitmq HA(active/active) without any rabbitmq load balancer.
(http://www.rabbitmq.com/ha.html)

		...
	    "rabbitmq" : {
	    	"addresses" : [
	        	{
	        		"host" : "rabbitmq-host1",
	        		"port" : 5672
	        	},
	        	{
	        		"host" : "rabbitmq-host2",
	        		"port" : 5672
	        	}
	        ],
	        "user" : "guest",
	        "pass" : "guest",
	        "vhost" : "/",
	        ...
		}
		...

Success / failure reporting via RabbitMQ can optionally be configured.
Default behaviour is currently to respond to all messages with Channel#basicAck, regardless of any processing
errors within Elasticsearch.
2 approaches can be conceived for reporting errors:
- use of the RabbitMQ Dead Letter Exchanges functionality (http://www.rabbitmq.com/dlx.html).
- posting exception messages to a custom Q.

Making use of the Dead Letter Exchanges feature of RabbitMQ results in rejected messages being moved to an alternate exchange
with an added header giving time of rejection. No additional content can be added, so it is not possible to add any diagnostic
info - correlation with ElasticSearch log files must be done in order to diagnose issues. When bulk_size configuraiton is such
that incoming messages will be concatenated into a single larger batches, all items in a failed batch are rejected, even though
some may have succeeded.


Using a custom Q, has the advantage of enabling the inclusion an error description within
the reported error message - can save time doing log correlation when diagnosing issues.

Below shows default values for the extended config:

	curl -XPUT 'localhost:9200/_river/my_river/_meta' -d '{
	    "type" : "rabbitmq",
	    "rabbitmq" : {
	        "host" : "localhost",
	        "port" : 5672,
	        "user" : "guest",
	        "pass" : "guest",
	        "vhost" : "/",
	        "queue" : "elasticsearch",
	        "exchange" : "elasticsearch",
	        "routing_key" : "elasticsearch",
	        "exchange_type" : "direct",
	        "exchange_durable" : true,
	        "queue_durable" : true,
	        "queue_auto_delete" : false
	    },
        "responseChannel" : {
            "response_messages_enabled" : false,
            "response_app_id" : "elasticsearch",
            "host" : "localhost",
            "port" : 5672,
            "user" : "guest",
            "pass" : "guest",
            "vhost" : "/",
            "queue" : "elasticsearch_response",
            "exchange" : "elasticsearch",
            "routing_key" : "elasticsearch",
            "exchange_type" : "direct",
            "exchange_durable" : true,
            "queue_durable" : true,
            "queue_auto_delete" : false
    },
	    "index" : {
	        "bulk_size" : 100,
	        "bulk_timeout" : "10ms",
	        "ordered" : false
	    }
	}'

Set 'response_messages_enabled = true' to enable response message sending.

Setting 'bulk_size=1' makes failure reporting un-ambiguous. With larger batch sizes, when we get failures in a batch, we send
response messages for every message added to the batch - some will have failed, some may not have done.

The river is automatically bulking queue messages if the queue is overloaded, allowing for faster catchup with the messages streamed into the queue. The `ordered` flag allows to make sure that the messages will be indexed in the same order as they arrive in the query by blocking on the bulk request before picking up the next data to be indexed. It can also be used as a simple way to throttle indexing.

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Original Copyright 2009-2012 Shay Banon and ElasticSearch <http://www.elasticsearch.org>
    Copyright Andy Olliver - for extensions related to response messages.

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
