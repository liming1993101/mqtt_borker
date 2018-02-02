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

import com.sh.wd.bean.MessageLogEntity;
import com.sh.wd.bean.PublishEvent;
import com.sh.wd.interception.messages.MoquetteMessage;
import com.sh.wd.server.ConnectionDescriptorStore;
import com.sh.wd.server.netty.NettyUtils;
import com.sh.wd.spi.IMessagesStore;
import com.sh.wd.spi.impl.subscriptions.Topic;
import com.sh.wd.spi.security.DBService;
import com.sh.wd.spi.security.IAuthorizator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Date;

import static com.sh.wd.spi.impl.ProtocolProcessor.asStoredMessage;
import static com.sh.wd.spi.impl.Utils.readBytesAndRewind;
import static io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;

public class Qos1PublishHandler extends QosPublishHandler {

    private static final Logger LOG = LoggerFactory.getLogger(Qos1PublishHandler.class);

    private final IMessagesStore m_messagesStore;
    private final BrokerInterceptor m_interceptor;
    private final ConnectionDescriptorStore connectionDescriptors;
    private final MessagesPublisher publisher;
    private DBService authenticator;

    public Qos1PublishHandler(IAuthorizator authorizator, DBService authenticator, IMessagesStore messagesStore, BrokerInterceptor interceptor,
                              ConnectionDescriptorStore connectionDescriptors, MessagesPublisher messagesPublisher) {
        super(authorizator);
        this.m_messagesStore = messagesStore;
        this.m_interceptor = interceptor;
        this.connectionDescriptors = connectionDescriptors;
        this.publisher = messagesPublisher;
        this.authenticator=authenticator;
    }

    void receivedPublishQos1(Channel channel, MqttPublishMessage msg) {
        // verify if topic can be write
        final Topic topic = new Topic(msg.variableHeader().topicName());
        String clientID = NettyUtils.clientID(channel);
        String username = NettyUtils.userName(channel);
        final int messageID = msg.variableHeader().packetId();
        Integer sendPackageID;
        // route message to subscribers
        String publishKey;
//
//        IMessagesStore.StoredMessage toStoreMsg = asStoredMessage(msg);
//        toStoreMsg.setClientID(clientID);
//        this.publisher.publish2Subscribers(toStoreMsg, topic, messageID);
//        if (msg.fixedHeader().isRetain()) {
//            if (!msg.payload().isReadable()) {
//                m_messagesStore.cleanRetained(topic);
//            } else {
//                // before wasn't stored
//                m_messagesStore.storeRetained(topic, toStoreMsg);
//            }
//        }


        LOG.info("读之前reader:"+msg.content().readerIndex()+" writer"+msg.content().writerIndex());
        ByteBuf byteBuf =  Unpooled.buffer();
        byte[] bytes = readBytesAndRewind(msg.content());
        msg.content().readBytes(bytes);
        String str="";
        try {
           str=new String(bytes,"UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        LOG.info("读之后reader:"+msg.content().readerIndex()+" writer"+msg.content().writerIndex());
        msg.content().readerIndex(0);
        LOG.info("重写reader:"+msg.content().readerIndex()+" writer"+msg.content().writerIndex());
        LOG.info("接收到"+clientID+"发送给"+topic.toString()+"消息:"+str);
        MessageLogEntity entity=new MessageLogEntity();
        entity.setContent(str);
        entity.setState(0);
        entity.setSender(clientID);
        entity.setReceiver(msg.variableHeader().topicName());
        entity.setSendTime(new Date().toString());

        sendPackageID=authenticator.storeMessage(entity);
        IMessagesStore.StoredMessage toStoreMsg = asStoredMessage(msg);
        toStoreMsg.setClientID(clientID);

        this.publisher.publish2Subscribers(toStoreMsg, topic, messageID);
        sendPubAck(clientID, messageID);


        publishKey = String.format("%s%d",topic.toString(),sendPackageID);//针对每个重生成key，保证消息ID不会重复
     //将ByteBuf转变为byte[]
      PublishEvent storePublishEvent = new PublishEvent(topic.toString(), MqttQoS.EXACTLY_ONCE,bytes, false,topic.toString(), sendPackageID);



      //存临时Publish消息，用于重发
       m_messagesStore.storeQosPublishMessage(publishKey, storePublishEvent);
      //开启Publish重传任务，在制定时间内未收到PubAck包则重传该条Publish信息
//        Map<String, Object> jobParam = new HashMap<String, Object>();
//      jobParam.put("ProtocolProcess", this);
//      jobParam.put("publishKey", publishKey);
//      QuartzManager.addJob(publishKey, "publish", publishKey, "publish", RePublishJob.class, 20, 1, jobParam);

      //从会话列表中取出会话，然后通过此会话发送publish消息
//       MqttFixedHeader fixedHeader=new MqttFixedHeader(MqttMessageType.PUBLISH,false,MqttQoS.AT_LEAST_ONCE,true,0);
        MqttPublishVariableHeader publishVariableHeader=new MqttPublishVariableHeader(topic.toString(),sendPackageID);
//        MQTTPublishMessage publishMessage=new MQTTPublishMessage(fixedHeader,publishVariableHeader,byteBuf);
//        publishMessage.retain();

//        this.connectionDescriptors.sendMessage(publishMessage,sendPackageID,msg.variableHeader().topicName());
//        msg.content().retain();

        MoquetteMessage moquetteMessage = new MoquetteMessage(msg.fixedHeader(), publishVariableHeader,msg.content());
        m_interceptor.notifyTopicPublished(moquetteMessage, clientID, username);
        msg.content().release();
//		if (!sub.isCleanSession()) {
        //将ByteBuf转变为byte[]
//        PublishEvent newPublishEvt = new PublishEvent(topic.toString(), MqttQoS.AT_LEAST_ONCE, bytes,
//        true, topic.toString(),
//        sendPackageID != null ? sendPackageID : 0);
        //如果发送的用户不在线 加入离线消息

//        m_interceptor.notifyTopicPublished(null, topic.toString(),username);
        if (!this.connectionDescriptors.isConnected(topic.toString())) {
            m_messagesStore.storeMessageToSessionForPublish(storePublishEvent);
        }
//        msg.release();
//       MoquetteMessage moquetteMessage = new MoquetteMessage(msg.fixedHeader(), msg.variableHeader(), msg.content());
//        m_interceptor.notifyTopicPublished(publishMessage, msg.variableHeader().topicName(), username);
    }

    private void sendPubAck(String clientId, int messageID) {
        LOG.trace("sendPubAck invoked");
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, true, AT_MOST_ONCE, true, 0);
        MqttPubAckMessage pubAckMessage = new MqttPubAckMessage(fixedHeader, from(messageID));

        try {
            if (connectionDescriptors == null) {
                throw new RuntimeException(
                        "Internal bad error, found connectionDescriptors to null while it should " + "be initialized, somewhere it's overwritten!!");
            }
            if(LOG.isDebugEnabled()){
                LOG.debug("clientIDs are {}", connectionDescriptors);
            }
            if (!connectionDescriptors.isConnected(clientId)) {
                throw new RuntimeException(
                        String.format("Can't find a ConnectionDescriptor for client %s in cache %s", clientId, connectionDescriptors));
            }
            connectionDescriptors.sendMessage(pubAckMessage, messageID, clientId);
        } catch (Throwable t) {
            LOG.error(null, t);
        }
    }

    /**
     * 在未收到对应包的情况下，重传Publish消息
     * @param publishKey
     * @author zer0
     * @version 1.0
     * @date 2015-11-28
     */
    public void reUnKnowPublishMessage(String publishKey){
        LOG.info("重新发送{" + publishKey + "}的Publish离线消息");
        PublishEvent pubEvent = m_messagesStore.searchQosPublishMessage(publishKey);
        if (pubEvent!=null) {
            try {
                MqttFixedHeader fixedHeader=new MqttFixedHeader(MqttMessageType.PUBLISH,false,MqttQoS.AT_LEAST_ONCE,false,0);
                MqttPublishVariableHeader publishVariableHeader=new MqttPublishVariableHeader(pubEvent.getTopic(),pubEvent.getPackgeID());
                MqttPublishMessage publishMessage=new MqttPublishMessage(fixedHeader,publishVariableHeader, Unpooled.buffer().writeBytes(pubEvent.getMessage()));
                this.connectionDescriptors.sendMessage(publishMessage,pubEvent.getPackgeID(),pubEvent.getTopic());
            } catch (NullPointerException e) {

            }
        }else {
            LOG.info("该条消息已被移除");
        }

    }
}
