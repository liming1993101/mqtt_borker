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

package com.sh.wd.persistence.memory;

import com.sh.wd.bean.PubRelEvent;
import com.sh.wd.bean.PublishEvent;
import com.sh.wd.spi.IMatchingCondition;
import com.sh.wd.spi.IMessagesStore;
import com.sh.wd.spi.impl.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MemoryMessagesStore implements IMessagesStore {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryMessagesStore.class);

    private Map<Topic, StoredMessage> m_retainedStore = new HashMap<>();

    MemoryMessagesStore() {
    }

    @Override
    public void initStore() {
    }

    @Override
    public void storeRetained(Topic topic, StoredMessage storedMessage) {
        if(LOG.isDebugEnabled()){
            LOG.debug("Store retained message for topic={}, CId={}", topic, storedMessage.getClientID());
        }
        if (storedMessage.getClientID() == null) {
            throw new IllegalArgumentException( "Message to be persisted must have a not null client ID");
        }
        m_retainedStore.put(topic, storedMessage);
    }

    @Override
    public List<PublishEvent> listMessagesInSession(String clientID) {
        return null;
    }

    @Override
    public void removeMessageInSessionForPublish(String clientID, Integer packgeID) {

    }

    @Override
    public void storeMessageToSessionForPublish(PublishEvent pubEvent) {

    }

    @Override
    public void storePublicPackgeID(String clientID, Integer packgeID) {

    }

    @Override
    public void removePublicPackgeID(String clientID) {

    }

    @Override
    public void removePubRecPackgeID(String clientID) {

    }

    @Override
    public void storePubRecPackgeID(String clientID, Integer packgeID) {

    }

    @Override
    public void storeQosPublishMessage(String publishKey, PublishEvent pubEvent) {

    }

    @Override
    public void removeQosPublishMessage(String publishKey) {

    }

    @Override
    public PublishEvent searchQosPublishMessage(String publishKey) {
        return null;
    }

    @Override
    public void storePubRelMessage(String pubRelKey, PubRelEvent pubRelEvent) {

    }

    @Override
    public void removePubRelMessage(String pubRelKey) {

    }

    @Override
    public PubRelEvent searchPubRelMessage(String pubRelKey) {
        return null;
    }

    @Override
    public void storeRetained(String topic, ByteBuf message, MqttQoS qos) {

    }

    @Override
    public void cleanRetained(String topic) {

    }

    @Override
    public Collection<StoredMessage> searchRetained(String topic) {
        return null;
    }

    @Override
    public Collection<StoredMessage> searchMatching(IMatchingCondition condition) {
        if(LOG.isDebugEnabled()){
            LOG.debug("searchMatching scanning all retained messages, presents are {}", m_retainedStore.size());
        }
        List<StoredMessage> results = new ArrayList<>();

        for (Map.Entry<Topic, StoredMessage> entry : m_retainedStore.entrySet()) {
            StoredMessage storedMsg = entry.getValue();
            if (condition.match(entry.getKey())) {
                results.add(storedMsg);
            }
        }

        return results;
    }

    @Override
    public void cleanRetained(Topic topic) {
        m_retainedStore.remove(topic);
    }
}
