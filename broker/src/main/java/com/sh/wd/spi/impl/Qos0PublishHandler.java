/*
 * Copyright (c) 2012-2017 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package com.sh.wd.spi.impl;

import com.sh.wd.interception.messages.MoquetteMessage;
import com.sh.wd.server.netty.NettyUtils;
import com.sh.wd.spi.IMessagesStore;
import com.sh.wd.spi.impl.subscriptions.Topic;
import com.sh.wd.spi.security.IAuthorizator;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Qos0PublishHandler extends QosPublishHandler {

    private static final Logger LOG = LoggerFactory.getLogger(Qos0PublishHandler.class);

    private final IMessagesStore m_messagesStore;
    private final BrokerInterceptor m_interceptor;
    private final MessagesPublisher publisher;

    public Qos0PublishHandler(IAuthorizator authorizator, IMessagesStore messagesStore, BrokerInterceptor interceptor,
                              MessagesPublisher messagesPublisher) {
        super(authorizator);
        this.m_messagesStore = messagesStore;
        this.m_interceptor = interceptor;
        this.publisher = messagesPublisher;
    }

    void receivedPublishQos0(Channel channel, MqttPublishMessage msg) {
        // verify if topic can be write
        final Topic topic = new Topic(msg.variableHeader().topicName());
        String clientID = NettyUtils.clientID(channel);
        String username = NettyUtils.userName(channel);
        if (!m_authorizator.canWrite(topic, username, clientID)) {
            LOG.error("MQTT client is not authorized to publish on topic. CId={}, topic={}", clientID, topic);
            return;
        }

        // route message to subscribers
        IMessagesStore.StoredMessage toStoreMsg = ProtocolProcessor.asStoredMessage(msg);
        toStoreMsg.setClientID(clientID);

        this.publisher.publish2Subscribers(toStoreMsg, topic);

        if (msg.fixedHeader().isRetain()) {
            // QoS == 0 && retain => clean old retained
            m_messagesStore.cleanRetained(topic);
        }

        // 修改publish消息，slice出的ByteBuf对象，原文中存在内存泄漏
        MoquetteMessage moquetteMessage = new MoquetteMessage(msg.fixedHeader(), msg.variableHeader(), msg.content());
        m_interceptor.notifyTopicPublished(moquetteMessage, clientID, username);
        msg.content().release();
    }
}
