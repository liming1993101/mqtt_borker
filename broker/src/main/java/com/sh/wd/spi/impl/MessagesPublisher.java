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

import com.sh.wd.spi.ISessionsStore;
import com.sh.wd.spi.impl.subscriptions.Subscription;
import com.sh.wd.spi.impl.subscriptions.SubscriptionsDirectory;
import com.sh.wd.spi.impl.subscriptions.Topic;
import com.sh.wd.server.ConnectionDescriptorStore;
import com.sh.wd.spi.ClientSession;
import com.sh.wd.spi.IMessagesStore;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

class MessagesPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(MessagesPublisher.class);
    private final ConnectionDescriptorStore connectionDescriptors;
    private final ISessionsStore m_sessionsStore;
    private final PersistentQueueMessageSender messageSender;
    private final SubscriptionsDirectory subscriptions;

    public MessagesPublisher(ConnectionDescriptorStore connectionDescriptors, ISessionsStore sessionsStore,
                             PersistentQueueMessageSender messageSender, SubscriptionsDirectory subscriptions) {
        this.connectionDescriptors = connectionDescriptors;
        this.m_sessionsStore = sessionsStore;
        this.messageSender = messageSender;
        this.subscriptions = subscriptions;
    }

    static MqttPublishMessage notRetainedPublish(String topic, MqttQoS qos, ByteBuf message) {
        return notRetainedPublishWithMessageId(topic, qos, message, 0);
    }

    private static MqttPublishMessage notRetainedPublishWithMessageId(String topic, MqttQoS qos, ByteBuf message,
            int messageId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, false, 0);
        MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(topic, messageId);
        return new MqttPublishMessage(fixedHeader, varHeader, message);
    }

    void publish2Subscribers(IMessagesStore.StoredMessage pubMsg, Topic topic, int messageID) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Sending publish message to subscribers. ClientId={}, topic={}, messageId={}, payload={}, " +
                    "subscriptionTree={}", pubMsg.getClientID(), topic, messageID, DebugUtils.payload2Str(pubMsg.getPayload()),
                subscriptions.dumpTree());
        } else if(LOG.isInfoEnabled()){
            LOG.info("Sending publish message to subscribers. ClientId={}, topic={}, messageId={}", pubMsg.getClientID(), topic,
                messageID);
        }
        publish2Subscribers(pubMsg, topic);
    }

    void publish2Subscribers(IMessagesStore.StoredMessage pubMsg, Topic topic) {
        List<Subscription> topicMatchingSubscriptions = subscriptions.matches(topic);
        final String topic1 = pubMsg.getTopic();
        final MqttQoS publishingQos = pubMsg.getQos();
        final ByteBuf origPayload = pubMsg.getPayload();

        for (final Subscription sub : topicMatchingSubscriptions) {
            MqttQoS qos = ProtocolProcessor.lowerQosToTheSubscriptionDesired(sub, publishingQos);
            ClientSession targetSession = m_sessionsStore.sessionForClient(sub.getClientId());

            boolean targetIsActive = this.connectionDescriptors.isConnected(sub.getClientId());
//TODO move all this logic into messageSender, which puts into the flightZone only the messages that pull out of the queue.
            if (targetIsActive) {
                if(LOG.isDebugEnabled()){
                    LOG.debug("Sending PUBLISH message to active subscriber. CId={}, topicFilter={}, qos={}",
                        sub.getClientId(), sub.getTopicFilter(), qos);
                }
                // we need to retain because duplicate only copy r/w indexes and don't retain() causing
                // refCnt = 0
                ByteBuf payload = origPayload.retainedDuplicate();
                MqttPublishMessage publishMsg;
                if (qos != MqttQoS.AT_MOST_ONCE) {
                    // QoS 1 or 2
                    int messageId = targetSession.inFlightAckWaiting(pubMsg);
                    // set the PacketIdentifier only for QoS > 0
                    publishMsg = notRetainedPublishWithMessageId(topic1, qos, payload, messageId);
                } else {
                    publishMsg = notRetainedPublish(topic1, qos, payload);
                }
                this.messageSender.sendPublish(targetSession, publishMsg);
            } else {
                if (!targetSession.isCleanSession()) {
                    if(LOG.isDebugEnabled()){
                        LOG.debug("Storing pending PUBLISH inactive message. CId={}, topicFilter={}, qos={}",
                            sub.getClientId(), sub.getTopicFilter(), qos);
                    }
                    // store the message in targetSession queue to deliver
                    targetSession.enqueue(pubMsg);
                }
            }
        }
    }

}
