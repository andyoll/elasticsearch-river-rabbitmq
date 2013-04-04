/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.rabbitmq;

import com.rabbitmq.client.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class RabbitmqRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private final RabbitmqRiverConfigHolder configHolder;
    private volatile boolean closed = false;
    private volatile Thread thread;
    private volatile RabbitmqConsumer rabbitmqConsumer;
    private RabbitmqConsumerFactory rabbitmqConsumerFactory;

    // @SuppressWarnings({"unchecked"})
    @Inject
    public RabbitmqRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;
        rabbitmqConsumerFactory = new RabbitmqConsumerFactory();

        Map<String, Object> rabbitSettings;
        if (settings.settings().containsKey("rabbitmq")) {
            rabbitSettings = (Map<String, Object>) settings.settings().get("rabbitmq");
        } else {
            // use default settings - pass in empty settings Hash
            rabbitSettings = new HashMap<String, Object>();
        }

        Map<String, Object> responseChannelSettings;
                if (settings.settings().containsKey("responseChannel")) {
                    responseChannelSettings = (Map<String, Object>) settings.settings().get("responseChannel");
                } else {
                    // use default settings - pass in empty settings Hash
                    responseChannelSettings = new HashMap<String, Object>();
                }

        Map<String, Object> indexSettings;
        if (settings.settings().containsKey("index")) {
            indexSettings = (Map<String, Object>) settings.settings().get("index");
        } else {
            // use default settings - pass in empty settings Hash
            indexSettings = new HashMap<String, Object>();
        }

        configHolder = new RabbitmqRiverConfigHolder(rabbitSettings, responseChannelSettings, indexSettings);
    }

    /**
     * Setter to allow injection of alternate (non default) factories to enable unit testing.
     *
     * @param rabbitmqConsumerFactory
     */
    void setRabbitmqConsumerFactory(RabbitmqConsumerFactory rabbitmqConsumerFactory) {
        this.rabbitmqConsumerFactory = rabbitmqConsumerFactory;
    }

    @Override
    public void start() {
        logger.info("creating rabbitmq river, addresses [{}], user [{}], vhost [{}]",
                configHolder.getIndexAddresses(),
                configHolder.getIndexUser(),
                configHolder.getIndexVhost());

        rabbitmqConsumer = rabbitmqConsumerFactory.newConsumer();
        rabbitmqConsumer.setLogger(logger);
        rabbitmqConsumer.setConfig(configHolder);
        rabbitmqConsumer.setConnectionFactory(new ConnectionFactory());
        rabbitmqConsumer.setQueueingConsumerFactory(new QueueingConsumerFactory());
        rabbitmqConsumer.setClient(client);

        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "rabbitmq_river").newThread(rabbitmqConsumer);
        thread.start();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing rabbitmq river");
        closed = true;
        rabbitmqConsumer.setClosed();
        thread.interrupt();
    }

    public Thread.State getWorkerThreadState() {
        if (thread != null) {
            return thread.getState();
        } else {
            return null;
        }
    }

}
