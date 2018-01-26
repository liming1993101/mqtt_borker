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

package com.sh.wd.spi;

import com.sh.wd.bean.PubRelEvent;
import com.sh.wd.bean.PublishEvent;
import com.sh.wd.spi.impl.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Defines the SPI to be implemented by a StorageService that handle persistence of messages
 */
public interface IMessagesStore {

    class StoredMessage implements Serializable {

        private static final long serialVersionUID = 1755296138639817304L;
        final MqttQoS m_qos;
        final byte[] m_payload;
        final String m_topic;
        private boolean m_retained;
        private String m_clientID;
        private MessageGUID m_guid;

        public StoredMessage(byte[] message, MqttQoS qos, String topic) {
            m_qos = qos;
            m_payload = message;
            m_topic = topic;
        }

        public MqttQoS getQos() {
            return m_qos;
        }

        public String getTopic() {
            return m_topic;
        }

        public void setGuid(MessageGUID guid) {
            this.m_guid = guid;
        }

        public MessageGUID getGuid() {
            return m_guid;
        }

        public String getClientID() {
            return m_clientID;
        }

        public void setClientID(String m_clientID) {
            this.m_clientID = m_clientID;
        }

        public ByteBuf getPayload() {
            return Unpooled.copiedBuffer(m_payload);
        }

        public void setRetained(boolean retained) {
            this.m_retained = retained;
        }

        public boolean isRetained() {
            return m_retained;
        }

        @Override
        public String toString() {
            return "PublishEvent{clientID='" + m_clientID + '\'' + ", m_retain="
                    + m_retained + ", m_qos=" + m_qos + ", m_topic='" + m_topic + '\'' + '}';
        }
    }

    /**
     * Used to initialize all persistent store structures
     */
    void initStore();

    /**
     * Return a list of retained messages that satisfy the condition.
     *
     * @param condition
     *            the condition to match during the search.
     * @return the collection of matching messages.
     */
    Collection<StoredMessage> searchMatching(IMatchingCondition condition);

    void cleanRetained(Topic topic);

    void storeRetained(Topic topic, StoredMessage storedMessage);




    /**
     * 返回某个clientID的离线消息列表
     * @param clientID
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    List<PublishEvent> listMessagesInSession(String clientID);


    /**
     * 在重发以后，移除publish的离线消息事件
     * @param clientID
     * @param packgeID
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    void removeMessageInSessionForPublish(String clientID, Integer packgeID);

    /**
     * 存储publish的离线消息事件，为CleanSession=0的情况做重发准备
     * @param pubEvent
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    void storeMessageToSessionForPublish(PublishEvent pubEvent);

    /**
     * 存储Publish的包ID
     * @param clientID
     * @param packgeID
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    void storePublicPackgeID(String clientID, Integer packgeID);

    /**
     * 移除Publish的包ID
     * @param clientID
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    void removePublicPackgeID(String clientID);

    /**
     * 移除PubRec的包ID
     * @param clientID
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    void removePubRecPackgeID(String clientID);

    /**
     * 存储PubRec的包ID
     * @param clientID
     * @param packgeID
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    void storePubRecPackgeID(String clientID, Integer packgeID);

    /**
     * 当Qos>0的时候，临时存储Publish消息，用于重发
     * @param publishKey
     * @param pubEvent
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    void storeQosPublishMessage(String publishKey, PublishEvent pubEvent);

    /**
     * 在收到对应的响应包后，删除Publish消息的临时存储
     * @param publishKey
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    void removeQosPublishMessage(String publishKey);

    /**
     * 获取临时存储的Publish消息，在等待时间过后未收到对应的响应包，则重发该Publish消息
     * @param publishKey
     * @return PublishEvent
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    PublishEvent searchQosPublishMessage(String publishKey);

    /**
     * 当Qos=2的时候，临时存储PubRel消息，在未收到PubComp包时用于重发
     * @param pubRelKey
     * @param pubRelEvent
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    void storePubRelMessage(String pubRelKey, PubRelEvent pubRelEvent);

    /**
     * 在收到对应的响应包后，删除PubRel消息的临时存储
     * @param pubRelKey
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    void removePubRelMessage(String pubRelKey);

    /**
     * 获取临时存储的PubRel消息，在等待时间过后未收到对应的响应包，则重发该PubRel消息
     * @param pubRelKey
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    PubRelEvent searchPubRelMessage(String pubRelKey);

    /**
     * 持久化存储保留Retain为1的指定topic的最新信息，该信息会在新客户端订阅某主题的时候发送给此客户端
     * @param topic
     * @param message
     * @param qos
     * @author lm
     * @version 1.0
     * @date 2015-11-26
     */
    void storeRetained(String topic, ByteBuf message, MqttQoS qos);

    /**
     * 删除指定topic的Retain信息
     * @param topic
     * @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    void cleanRetained(String topic);

    /**
     * 从Retain中搜索对应topic中保存的信息
     * @param topic
     ** @author lm
     * @version 1.0
     * @date 2017-11-18
     */
    Collection<StoredMessage> searchRetained(String topic);

}
