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

import com.sh.wd.bean.PubRelEvent;
import com.sh.wd.bean.PublishEvent;
import com.sh.wd.interception.InterceptHandler;
import com.sh.wd.server.ConnectionDescriptor;
import com.sh.wd.server.ConnectionDescriptorStore;
import com.sh.wd.server.netty.NettyUtils;
import com.sh.wd.server.netty.TimeOutHandle;
import com.sh.wd.spi.*;
import com.sh.wd.spi.impl.subscriptions.Subscription;
import com.sh.wd.spi.impl.subscriptions.SubscriptionsDirectory;
import com.sh.wd.spi.impl.subscriptions.Topic;
import com.sh.wd.spi.security.DBService;
import com.sh.wd.spi.security.IAuthorizator;
import com.sh.wd.utils.QuartzManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.netty.handler.codec.mqtt.MqttConnectReturnCode.*;
import static io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from;
import static io.netty.handler.codec.mqtt.MqttQoS.*;

/**
 * Class responsible to handle the logic of MQTT protocol it's the director of the protocol
 * execution.
 *
 * Used by the front facing class ProtocolProcessorBootstrapper.
 */
public class ProtocolProcessor {

    static final class WillMessage {

        private final String topic;
        private final ByteBuffer payload;
        private final boolean retained;
        private final MqttQoS qos;

        WillMessage(String topic, ByteBuffer payload, boolean retained, MqttQoS qos) {
            this.topic = topic;
            this.payload = payload;
            this.retained = retained;
            this.qos = qos;
        }

        public String getTopic() {
            return topic;
        }

        public ByteBuffer getPayload() {
            return payload;
        }

        public boolean isRetained() {
            return retained;
        }

        public MqttQoS getQos() {
            return qos;
        }
    }

    private enum SubscriptionState {
        STORED, VERIFIED
    }

    private class RunningSubscription {

        final String clientID;
        final long packetId;

        RunningSubscription(String clientID, long packetId) {
            this.clientID = clientID;
            this.packetId = packetId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            RunningSubscription that = (RunningSubscription) o;

            return packetId == that.packetId
                    && (clientID != null ? clientID.equals(that.clientID) : that.clientID == null);
        }

        @Override
        public int hashCode() {
            int result = clientID != null ? clientID.hashCode() : 0;
            result = 31 * result + (int) (packetId ^ (packetId >>> 32));
            return result;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolProcessor.class);

    protected ConnectionDescriptorStore connectionDescriptors;
    protected ConcurrentMap<RunningSubscription, SubscriptionState> subscriptionInCourse;

    private SubscriptionsDirectory subscriptions;
    private ISubscriptionsStore subscriptionStore;
    private boolean allowAnonymous;
    private boolean allowZeroByteClientId;
    private IAuthorizator m_authorizator;

    private IMessagesStore m_messagesStore;

    private ISessionsStore m_sessionsStore;

    private DBService dbService;
    private BrokerInterceptor m_interceptor;

    private Qos0PublishHandler qos0PublishHandler;
    private Qos1PublishHandler qos1PublishHandler;
    private Qos2PublishHandler qos2PublishHandler;
    private MessagesPublisher messagesPublisher;
    private InternalRepublisher internalRepublisher;

    // maps clientID to Will testament, if specified on CONNECT
    private ConcurrentMap<String, WillMessage> m_willStore = new ConcurrentHashMap<>();

    ProtocolProcessor() {
    }

    public void init(SubscriptionsDirectory subscriptions, IMessagesStore storageService, ISessionsStore sessionsStore,
                     DBService authenticator, boolean allowAnonymous, IAuthorizator authorizator,
                     BrokerInterceptor interceptor) {
        init(subscriptions, storageService, sessionsStore, authenticator, allowAnonymous, false, authorizator,
            interceptor, null);
    }

    public void init(SubscriptionsDirectory subscriptions, IMessagesStore storageService, ISessionsStore sessionsStore,
                     DBService authenticator, boolean allowAnonymous, boolean allowZeroByteClientId,
                     IAuthorizator authorizator, BrokerInterceptor interceptor) {
        init(subscriptions, storageService, sessionsStore, authenticator, allowAnonymous, allowZeroByteClientId,
            authorizator, interceptor, null);
    }

    public void init(SubscriptionsDirectory subscriptions, IMessagesStore storageService, ISessionsStore sessionsStore,
                     DBService authenticator, boolean allowAnonymous, boolean allowZeroByteClientId,
                     IAuthorizator authorizator, BrokerInterceptor interceptor, String serverPort) {
        init(new ConnectionDescriptorStore(sessionsStore), subscriptions, storageService, sessionsStore, authenticator,
            allowAnonymous, allowZeroByteClientId, authorizator, interceptor, serverPort);
    }

    /**
     * @param subscriptions
     *            the subscription store where are stored all the existing clients subscriptions.
     * @param storageService
     *            the persistent store to use for save/load of messages for QoS1 and QoS2 handling.
     * @param sessionsStore
     *            the clients sessions store, used to persist subscriptions.
     * @param authenticator
     *            the authenticator used in connect messages.
     * @param allowAnonymous
     *            true connection to clients without credentials.
     * @param allowZeroByteClientId
     *            true to allow clients connect without a clientid
     * @param authorizator
     *            used to apply ACL policies to publishes and subscriptions.
     * @param interceptor
     *            to notify events to an intercept handler
     */
    void init(ConnectionDescriptorStore connectionDescriptors, SubscriptionsDirectory subscriptions,
              IMessagesStore storageService, ISessionsStore sessionsStore, DBService authenticator,
              boolean allowAnonymous, boolean allowZeroByteClientId, IAuthorizator authorizator,
              BrokerInterceptor interceptor, String serverPort) {
        LOG.info("Initializing MQTT protocol processor...");
        this.connectionDescriptors = connectionDescriptors;
        this.subscriptionInCourse = new ConcurrentHashMap<>();
        this.m_interceptor = interceptor;
        this.subscriptions = subscriptions;
        this.allowAnonymous = allowAnonymous;
        this.allowZeroByteClientId = allowZeroByteClientId;
        m_authorizator = authorizator;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Initial subscriptions tree={}", subscriptions.dumpTree());
        }
        dbService = authenticator;
        m_messagesStore = storageService;
        m_sessionsStore = sessionsStore;
        subscriptionStore = sessionsStore.subscriptionStore();

        LOG.info("Initializing messages publisher...");
        final PersistentQueueMessageSender messageSender = new PersistentQueueMessageSender(this.connectionDescriptors);
        this.messagesPublisher = new MessagesPublisher(connectionDescriptors, sessionsStore, messageSender,
            subscriptions);

        LOG.info("Initializing QoS publish handlers...");
        this.qos0PublishHandler = new Qos0PublishHandler(m_authorizator, m_messagesStore, m_interceptor,
                this.messagesPublisher);
        this.qos1PublishHandler = new Qos1PublishHandler(authorizator,dbService, m_messagesStore, m_interceptor,
                this.connectionDescriptors, this.messagesPublisher);
        this.qos2PublishHandler = new Qos2PublishHandler(m_authorizator, subscriptions, m_messagesStore, m_interceptor,
                this.connectionDescriptors, m_sessionsStore, this.messagesPublisher);

        LOG.info("Initializing internal republisher...");
        this.internalRepublisher = new InternalRepublisher(messageSender);
    }

    public void processConnect(Channel channel, MqttConnectMessage msg) {
        MqttConnectPayload payload = msg.payload();
        String clientId = payload.clientIdentifier();
        LOG.info("Processing CONNECT message. CId={}, username={}", clientId, payload.userName());

        if (msg.variableHeader().version() != MqttVersion.MQTT_3_1.protocolLevel()
                && msg.variableHeader().version() != MqttVersion.MQTT_3_1_1.protocolLevel()) {
            MqttConnAckMessage badProto = connAck(CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);

            LOG.error("MQTT protocol version is not valid. CId={}", clientId);
            channel.writeAndFlush(badProto);
            channel.close();
            return;
        }

        if (clientId == null || clientId.length() == 0) {
            if (!msg.variableHeader().isCleanSession() || !this.allowZeroByteClientId) {
                MqttConnAckMessage badId = connAck(CONNECTION_REFUSED_IDENTIFIER_REJECTED);

                channel.writeAndFlush(badId);
                channel.close();
                LOG.error("The MQTT client ID cannot be empty. Username={}", payload.userName());
                return;
            }

            // Generating client id.
            clientId = UUID.randomUUID().toString().replace("-", "");
            LOG.info("Client has connected with a server generated identifier. CId={}, username={}", clientId,
                payload.userName());
        }

        if (!login(channel, msg, clientId)) {
            channel.close();
            return;
        }

        LOG.info("用户登陆成功: CId={}, username={} ", clientId,
                payload.userName());
        ConnectionDescriptor descriptor = new ConnectionDescriptor(clientId, channel,
            msg.variableHeader().isCleanSession());
        ConnectionDescriptor existing = this.connectionDescriptors.addConnection(descriptor);


        initializeKeepAliveTimeout(channel, msg, clientId);
        if (existing != null) {
            LOG.info("The client ID is being used in an existing connection. It will be closed. CId={}", clientId);
            existing.abort();
        }

        storeWillMessage(msg, clientId);
        if (!sendAck(descriptor, msg, clientId)) {
            channel.close();
            return;
        }

        m_interceptor.notifyClientConnected(msg);

        final ClientSession clientSession = createOrLoadClientSession(descriptor, msg, clientId);
        if (clientSession == null) {
            channel.close();
            return;
        }

        if (!republish(descriptor, msg, clientSession)) {
            channel.close();
            return;
        }
        final boolean success = descriptor.assignState(ConnectionDescriptor.ConnectionState.MESSAGES_REPUBLISHED, ConnectionDescriptor.ConnectionState.ESTABLISHED);
        if (!success) {
            channel.close();
        }
        republishMessage(clientId);
        LOG.info("The CONNECT message has been processed. CId={}, username={}", clientId, payload.userName());
    }

    private MqttConnAckMessage connAck(MqttConnectReturnCode returnCode) {
        return connAck(returnCode, false);
    }

    private MqttConnAckMessage connAckWithSessionPresent(MqttConnectReturnCode returnCode) {
        return connAck(returnCode, true);
    }

    private MqttConnAckMessage connAck(MqttConnectReturnCode returnCode, boolean sessionPresent) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE,
                false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader = new MqttConnAckVariableHeader(returnCode, sessionPresent);
        return new MqttConnAckMessage(mqttFixedHeader, mqttConnAckVariableHeader);
    }

    private boolean login(Channel channel, MqttConnectMessage msg, final String clientId) {
        // handle user authentication
        if (msg.variableHeader().hasUserName()) {
            String pwd = null;
            if (msg.variableHeader().hasPassword()) {
                pwd = msg.payload().password();
            } else if (!this.allowAnonymous) {
                LOG.error("Client didn't supply any password and MQTT anonymous mode is disabled CId={}", clientId);
                failedCredentials(channel);
                return false;
            }

            if (dbService.login(clientId,pwd)==-1) {
                LOG.error("Authenticator has rejected the MQTT credentials CId={}, username={}, password={}",
                        clientId, msg.payload().userName(), pwd);
                failedCredentials(channel);
                return false;
            }
            NettyUtils.userName(channel, msg.payload().userName());
        } else if (!this.allowAnonymous) {
            LOG.error("Client didn't supply any credentials and MQTT anonymous mode is disabled. CId={}", clientId);
            failedCredentials(channel);
            return false;
        }
        dbService.addUserState(clientId,1);
        return true;
    }

    private boolean sendAck(ConnectionDescriptor descriptor, MqttConnectMessage msg, final String clientId) {
        LOG.info("Sending connect ACK. CId={}", clientId);
        final boolean success = descriptor.assignState(ConnectionDescriptor.ConnectionState.DISCONNECTED, ConnectionDescriptor.ConnectionState.SENDACK);
        if (!success) {
            return false;
        }

        MqttConnAckMessage okResp;
        ClientSession clientSession = m_sessionsStore.sessionForClient(clientId);
        boolean isSessionAlreadyStored = clientSession != null;
        if (!msg.variableHeader().isCleanSession() && isSessionAlreadyStored) {
            okResp = connAckWithSessionPresent(CONNECTION_ACCEPTED);
        } else {
            okResp = connAck(CONNECTION_ACCEPTED);
        }

        if (isSessionAlreadyStored) {
            LOG.info("Cleaning session. CId={}", clientId);
            clientSession.cleanSession(msg.variableHeader().isCleanSession());
        }
        descriptor.writeAndFlush(okResp);
        LOG.info("The connect ACK has been sent. CId={}", clientId);
        return true;
    }

    private void initializeKeepAliveTimeout(Channel channel, MqttConnectMessage msg, final String clientId) {
        int keepAlive = msg.variableHeader().keepAliveTimeSeconds();
        LOG.info("Configuring connection. CId={}", clientId);
        NettyUtils.keepAlive(channel, keepAlive);
        // session.attr(NettyUtils.ATTR_KEY_CLEANSESSION).set(msg.variableHeader().isCleanSession());
        NettyUtils.cleanSession(channel, msg.variableHeader().isCleanSession());
        // used to track the client in the subscription and publishing phases.
        // session.attr(NettyUtils.ATTR_KEY_CLIENTID).set(msg.getClientID());
        NettyUtils.clientID(channel, clientId);
        int idleTime = Math.round(keepAlive * 1.5f);
        setIdleTime(channel.pipeline(), idleTime,clientId);

        if(LOG.isDebugEnabled()){
            LOG.debug("The connection has been configured CId={}, keepAlive={}, cleanSession={}, idleTime={}",
                    clientId, keepAlive, msg.variableHeader().isCleanSession(), idleTime);
        }
    }

    private void storeWillMessage(MqttConnectMessage msg, final String clientId) {
        // Handle will flag
        if (msg.variableHeader().isWillFlag()) {
            MqttQoS willQos = MqttQoS.valueOf(msg.variableHeader().willQos());
            LOG.info("Configuring MQTT last will and testament CId={}, willQos={}, willTopic={}, willRetain={}",
                    clientId, willQos, msg.payload().willTopic(), msg.variableHeader().isWillRetain());
            byte[] willPayload = msg.payload().willMessageInBytes();
            ByteBuffer bb = (ByteBuffer) ByteBuffer.allocate(willPayload.length).put(willPayload).flip();
            // save the will testament in the clientID store
            WillMessage will = new WillMessage(msg.payload().willTopic(), bb, msg.variableHeader().isWillRetain(),
                    willQos);
            m_willStore.put(clientId, will);
            LOG.info("MQTT last will and testament has been configured. CId={}", clientId);
        }
    }

    private ClientSession createOrLoadClientSession(ConnectionDescriptor descriptor, MqttConnectMessage msg,
                                                    String clientId) {
        final boolean success = descriptor.assignState(ConnectionDescriptor.ConnectionState.SENDACK, ConnectionDescriptor.ConnectionState.SESSION_CREATED);
        if (!success) {
            return null;
        }

        ClientSession clientSession = m_sessionsStore.sessionForClient(clientId);
        boolean isSessionAlreadyStored = clientSession != null;
        if (!isSessionAlreadyStored) {
            clientSession = m_sessionsStore.createNewSession(clientId, msg.variableHeader().isCleanSession());
        }
        if (msg.variableHeader().isCleanSession()) {
            LOG.info("Cleaning session. CId={}", clientId);
            clientSession.cleanSession();
        }
        return clientSession;
    }

    private boolean republish(ConnectionDescriptor descriptor, MqttConnectMessage msg, ClientSession clientSession) {
        final boolean success = descriptor.assignState(ConnectionDescriptor.ConnectionState.SESSION_CREATED, ConnectionDescriptor.ConnectionState.MESSAGES_REPUBLISHED);
        if (!success) {
            return false;
        }

        if (!msg.variableHeader().isCleanSession()) {
            // force the republish of stored QoS1 and QoS2
            republishStoredInSession(clientSession);
        }
        int flushIntervalMs = 500/* (keepAlive * 1000) / 2 */;
        descriptor.setupAutoFlusher(flushIntervalMs);
        return true;
    }

    private void failedCredentials(Channel session) {
        session.writeAndFlush(connAck(CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD));
        LOG.info("Client {} failed to connect with bad username or password.", session);
    }

    private void setIdleTime(ChannelPipeline pipeline, int idleTime,String clientId) {
        if (pipeline.names().contains("idleStateHandler")) {
            pipeline.remove("idleStateHandler");
        }
        pipeline.addFirst("idleStateHandler", new TimeOutHandle(idleTime, 0, 0,clientId,dbService,this.connectionDescriptors));
    }

    /**
     * Republish QoS1 and QoS2 messages stored into the session for the clientID.
     */
    private void republishStoredInSession(ClientSession clientSession) {
        LOG.info("Republishing stored publish events. CId={}", clientSession.clientID);
        BlockingQueue<IMessagesStore.StoredMessage> publishedEvents = clientSession.queue();
        if (publishedEvents.isEmpty()) {
            LOG.info("There are no stored publish events to ClientId={}", clientSession.clientID);
            return;
        }

        this.internalRepublisher.publishStored(clientSession, publishedEvents);
    }

    public void processPubAck(Channel channel, MqttPubAckMessage msg) {
        String clientID = NettyUtils.clientID(channel);
        int messageID = msg.variableHeader().messageId();
//        String username = NettyUtils.userName(channel);
//        LOG.trace("retrieving inflight for messageID <{}>", messageID);
//
//        ClientSession targetSession = m_sessionsStore.sessionForClient(clientID);
//        StoredMessage inflightMsg = targetSession.inFlightAcknowledged(messageID);
//
//        if(inflightMsg!=null && inflightMsg.getTopic()!=null) {
//            String topic = inflightMsg.getTopic();
//            InterceptAcknowledgedMessage wrapped = new InterceptAcknowledgedMessage(inflightMsg, topic, username, messageID);
//            m_interceptor.notifyMessageAcknowledged(wrapped);
//        }
        boolean b= dbService.updataMessageState(messageID,1);//消息发送成功 修改MySQL数据库消息状态
        if (!b){
            LOG.error("客户端收到消息:","数据库修改消息状态失败!");
        }
        String publishKey = String.format("%s%d", clientID, messageID);
        //取消Publish重传任务
        QuartzManager.removeJob(publishKey, "publish", publishKey, "publish");
        //删除临时存储用于重发的Publish消息
        m_messagesStore.removeQosPublishMessage(publishKey);
        m_messagesStore.removeMessageInSessionForPublish(clientID,messageID);
        //最后把使用完的包ID释放掉
//        PackageIDManager.releaseMessageId(messageID);

    }

    public static IMessagesStore.StoredMessage asStoredMessage(MqttPublishMessage msg) {
        // TODO ugly, too much array copy
        ByteBuf payload = msg.payload();
        byte[] payloadContent = Utils.readBytesAndRewind(payload);

        IMessagesStore.StoredMessage stored = new IMessagesStore.StoredMessage(payloadContent,
                msg.fixedHeader().qosLevel(), msg.variableHeader().topicName());
        stored.setRetained(msg.fixedHeader().isRetain());
        return stored;
    }

    private static IMessagesStore.StoredMessage asStoredMessage(WillMessage will) {
        IMessagesStore.StoredMessage pub = new IMessagesStore.StoredMessage(will.getPayload().array(), will.getQos(),
                will.getTopic());
        pub.setRetained(will.isRetained());
        return pub;
    }

    public void processPublish(Channel channel, MqttPublishMessage msg) {
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        final String clientId = NettyUtils.clientID(channel);
        LOG.info("Processing PUBLISH message. CId={}, topic={}, messageId={}, qos={}", clientId,
                msg.variableHeader().topicName(), msg.variableHeader().packetId(), qos);
        switch (qos) {
            case AT_MOST_ONCE:
                this.qos0PublishHandler.receivedPublishQos0(channel, msg);
                break;
            case AT_LEAST_ONCE:
                this.qos1PublishHandler.receivedPublishQos1(channel, msg);
                break;
            case EXACTLY_ONCE:
                this.qos2PublishHandler.receivedPublishQos2(channel, msg);
                break;
            default:
                LOG.error("Unknown QoS-Type:{}", qos);
                break;
        }
    }

    /**
     * Intended usage is only for embedded versions of the broker, where the hosting application
     * want to use the broker to send a publish message. Inspired by {@link #processPublish} but
     * with some changes to avoid security check, and the handshake phases for Qos1 and Qos2. It
     * also doesn't notifyTopicPublished because using internally the owner should already know
     * where it's publishing.
     *
     * @param msg
     *            the message to publish.
     * @param clientId
     *            the clientID
     */
    public void internalPublish(MqttPublishMessage msg, final String clientId) {
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        final Topic topic = new Topic(msg.variableHeader().topicName());
        LOG.info("Sending PUBLISH message. Topic={}, qos={}", topic, qos);

        MessageGUID guid = null;
        IMessagesStore.StoredMessage toStoreMsg = asStoredMessage(msg);
        if (clientId == null || clientId.isEmpty()) {
            toStoreMsg.setClientID("BROKER_SELF");
        } else {
            toStoreMsg.setClientID(clientId);
        }
//        if (qos == EXACTLY_ONCE) { // QoS2
//            guid = m_messagesStore.storePublishForFuture(toStoreMsg);
//        }
        this.messagesPublisher.publish2Subscribers(toStoreMsg, topic);

        if (!msg.fixedHeader().isRetain()) {
            return;
        }
        if (qos == AT_MOST_ONCE || msg.payload().readableBytes() == 0) {
            // QoS == 0 && retain => clean old retained
            m_messagesStore.cleanRetained(topic);
            return;
        }
//        if (guid == null) {
//            // before wasn't stored
//            guid = m_messagesStore.storePublishForFuture(toStoreMsg);
//        }
        m_messagesStore.storeRetained(topic, toStoreMsg);
    }

    /**
     * Specialized version to publish will testament message.
     */
    private void forwardPublishWill(WillMessage will, String clientID) {
        LOG.info("Publishing will message. CId={}, topic={}", clientID, will.getTopic());
        // it has just to publish the message downstream to the subscribers
        // NB it's a will publish, it needs a PacketIdentifier for this conn, default to 1
        IMessagesStore.StoredMessage tobeStored = asStoredMessage(will);
        tobeStored.setClientID(clientID);
        Topic topic = new Topic(tobeStored.getTopic());
        this.messagesPublisher.publish2Subscribers(tobeStored, topic);
    }

    static MqttQoS lowerQosToTheSubscriptionDesired(Subscription sub, MqttQoS qos) {
        if (qos.value() > sub.getRequestedQos().value()) {
            qos = sub.getRequestedQos();
        }
        return qos;
    }

    /**
     * Second phase of a publish QoS2 protocol, sent by publisher to the broker. Search the stored
     * message and publish to all interested subscribers.
     *
     * @param channel
     *            the channel of the incoming message.
     * @param msg
     *            the decoded pubrel message.
     */
    public void processPubRel(Channel channel, MqttMessage msg) {
        this.qos2PublishHandler.processPubRel(channel, msg);
    }

    public void processPubRec(Channel channel, MqttMessage msg) {
        String clientID = NettyUtils.clientID(channel);
        ClientSession targetSession = m_sessionsStore.sessionForClient(clientID);
        // remove from the inflight and move to the QoS2 second phase queue
        int messageID = Utils.messageId(msg);
        IMessagesStore.StoredMessage ackedMsg = targetSession.inFlightAcknowledged(messageID);
        targetSession.moveInFlightToSecondPhaseAckWaiting(messageID, ackedMsg);
        // once received a PUBREC reply with a PUBREL(messageID)

        MqttFixedHeader pubRelHeader = new MqttFixedHeader(MqttMessageType.PUBREL, false, AT_LEAST_ONCE, false, 0);
        MqttMessage pubRelMessage = new MqttMessage(pubRelHeader, from(messageID));
        channel.writeAndFlush(pubRelMessage);
    }

    public void processPubComp(Channel channel, MqttMessage msg) {
//        String clientID = NettyUtils.clientID(channel);
          int messageID = Utils.messageId(msg);
//        if(LOG.isDebugEnabled()){
//            LOG.debug("Processing PUBCOMP message. CId={}, messageId={}", clientID, messageID);
//        }
//        // once received the PUBCOMP then remove the message from the temp memory
//        ClientSession targetSession = m_sessionsStore.sessionForClient(clientID);
//        StoredMessage inflightMsg = targetSession.secondPhaseAcknowledged(messageID);
//        String username = NettyUtils.userName(channel);
//        String topic = inflightMsg.getTopic();
//        m_interceptor.notifyMessageAcknowledged(new InterceptAcknowledgedMessage(inflightMsg, topic, username, messageID));

        String clientID = NettyUtils.clientID(channel);
        System.out.println("-----------------------收到消息"+clientID.toString()+"并要删除消息pubCom");

        String pubRelkey = String.format("%s%d", clientID, messageID);

        //删除存储的PubRec包ID
        m_messagesStore.removePubRecPackgeID(clientID);
        //取消PubRel的重传任务，删除临时存储的PubRel事件
        QuartzManager.removeJob(pubRelkey, "pubRel", pubRelkey, "pubRel");
        m_messagesStore.removePubRelMessage(pubRelkey);
        //最后把使用完的包ID释放掉
//        PackageIDManager.releaseMessageId(messageID);
    }

    public void processDisconnect(Channel channel) throws InterruptedException {
        final String clientID = NettyUtils.clientID(channel);
        LOG.info("Processing DISCONNECT message. CId={}", clientID);
        channel.flush();
        final ConnectionDescriptor existingDescriptor = this.connectionDescriptors.getConnection(clientID);
        if (existingDescriptor == null) {
            // another client with same ID removed the descriptor, we must exit
            channel.close();
            return;
        }

        if (existingDescriptor.doesNotUseChannel(channel)) {
            // another client saved it's descriptor, exit
            LOG.warn("Another client is using the connection descriptor. Closing connection. CId={}", clientID);
            existingDescriptor.abort();
            return;
        }

        if (!removeSubscriptions(existingDescriptor, clientID)) {
            LOG.warn("Unable to remove subscriptions. Closing connection. CId={}", clientID);
            existingDescriptor.abort();
            return;
        }

        if (!dropStoredMessages(existingDescriptor, clientID)) {
            LOG.warn("Unable to drop stored messages. Closing connection. CId={}", clientID);
            existingDescriptor.abort();
            return;
        }

        if (!cleanWillMessageAndNotifyInterceptor(existingDescriptor, clientID)) {
            LOG.warn("Unable to drop will message. Closing connection. CId={}", clientID);
            existingDescriptor.abort();
            return;
        }

        if (!existingDescriptor.close()) {
            LOG.info("The connection has been closed. CId={}", clientID);
            return;
        }

        boolean stillPresent = this.connectionDescriptors.removeConnection(existingDescriptor);
        if (!stillPresent) {
            // another descriptor was inserted
            LOG.warn("Another descriptor has been inserted. CId={}", clientID);
            return;
        }

        LOG.info("The DISCONNECT message has been processed. CId={}", clientID);
    }

    private boolean removeSubscriptions(ConnectionDescriptor descriptor, String clientID) {
        final boolean success = descriptor.assignState(ConnectionDescriptor.ConnectionState.ESTABLISHED, ConnectionDescriptor.ConnectionState.SUBSCRIPTIONS_REMOVED);
        if (!success) {
            return false;
        }

        if (descriptor.cleanSession) {
            LOG.info("Removing saved subscriptions. CId={}", descriptor.clientID);
            subscriptionStore.wipeSubscriptions(clientID);
            LOG.info("The saved subscriptions have been removed. CId={}", descriptor.clientID);
        }
        return true;
    }

    private boolean dropStoredMessages(ConnectionDescriptor descriptor, String clientID) {
        final boolean success = descriptor.assignState(ConnectionDescriptor.ConnectionState.SUBSCRIPTIONS_REMOVED, ConnectionDescriptor.ConnectionState.MESSAGES_DROPPED);
        if (!success) {
            return false;
        }

        if (descriptor.cleanSession) {
            if(LOG.isDebugEnabled()){
                LOG.debug("Removing messages of session. CId={}", descriptor.clientID);
            }
            this.m_sessionsStore.dropQueue(clientID);
            if(LOG.isDebugEnabled()){
                LOG.debug("The messages of the session have been removed. CId={}", descriptor.clientID);
            }
        }
        return true;
    }

    private boolean cleanWillMessageAndNotifyInterceptor(ConnectionDescriptor descriptor, String clientID) {
        final boolean success = descriptor.assignState(ConnectionDescriptor.ConnectionState.MESSAGES_DROPPED, ConnectionDescriptor.ConnectionState.INTERCEPTORS_NOTIFIED);
        if (!success) {
            return false;
        }

        LOG.info("Removing will message. ClientId={}", descriptor.clientID);
        // cleanup the will store
        m_willStore.remove(clientID);
        String username = descriptor.getUsername();
        m_interceptor.notifyClientDisconnected(clientID, username);
        return true;
    }

    public void processConnectionLost(String clientID, Channel channel) {
        LOG.info("Processing connection lost event. CId={}", clientID);
        ConnectionDescriptor oldConnDescr = new ConnectionDescriptor(clientID, channel, true);
        connectionDescriptors.removeConnection(oldConnDescr);
        // publish the Will message (if any) for the clientID
        if (m_willStore.containsKey(clientID)) {
            WillMessage will = m_willStore.get(clientID);
            forwardPublishWill(will, clientID);
            m_willStore.remove(clientID);
        }

        String username = NettyUtils.userName(channel);
        m_interceptor.notifyClientConnectionLost(clientID, username);
    }

    /**
     * Remove the clientID from topic subscription, if not previously subscribed, doesn't reply any
     * error.
     *
     * @param channel
     *            the channel of the incoming message.
     * @param msg
     *            the decoded unsubscribe message.
     */
    public void processUnsubscribe(Channel channel, MqttUnsubscribeMessage msg) {
        List<String> topics = msg.payload().topics();
        String clientID = NettyUtils.clientID(channel);

        LOG.info("Processing UNSUBSCRIBE message. CId={}, topics={}", clientID, topics);

        ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);
        for (String t : topics) {
            Topic topic = new Topic(t);
            boolean validTopic = topic.isValid();
            if (!validTopic) {
                // close the connection, not valid topicFilter is a protocol violation
                channel.close();
                LOG.error("Topic filter is not valid. CId={}, topics={}, badTopicFilter={}", clientID, topics, topic);
                return;
            }
            if(LOG.isDebugEnabled()){
                LOG.debug("Removing subscription. CId={}, topic={}", clientID, topic);
            }
            subscriptions.removeSubscription(topic, clientID);
            subscriptionStore.removeSubscription(topic,clientID);
            clientSession.unsubscribeFrom(topic);
            String username = NettyUtils.userName(channel);
            m_interceptor.notifyTopicUnsubscribed(topic.toString(), clientID, username);
        }

        // ack the client
        int messageID = msg.variableHeader().messageId();
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, AT_LEAST_ONCE, false, 0);
        MqttUnsubAckMessage ackMessage = new MqttUnsubAckMessage(fixedHeader, from(messageID));

        LOG.info("Sending UNSUBACK message. CId={}, topics={}, messageId={}", clientID, topics, messageID);
        channel.writeAndFlush(ackMessage);
    }

    public void processSubscribe(Channel channel, MqttSubscribeMessage msg) {
        String clientID = NettyUtils.clientID(channel);
        int messageID = Utils.messageId(msg);
        LOG.info("Processing SUBSCRIBE message. CId={}, messageId={}", clientID, messageID);

        RunningSubscription executionKey = new RunningSubscription(clientID, messageID);
        SubscriptionState currentStatus = subscriptionInCourse.putIfAbsent(executionKey, SubscriptionState.VERIFIED);
        if (currentStatus != null) {
            LOG.warn("Client sent another SUBSCRIBE message while this one was being processed CId={}, messageId={}",
                clientID, messageID);
            return;
        }
        String username = NettyUtils.userName(channel);
        List<MqttTopicSubscription> ackTopics = doVerify(clientID, username, msg);
        MqttSubAckMessage ackMessage = doAckMessageFromValidateFilters(ackTopics, messageID);
        if (!this.subscriptionInCourse.replace(executionKey, SubscriptionState.VERIFIED, SubscriptionState.STORED)) {
            LOG.warn("Client sent another SUBSCRIBE message while the topic filters were being verified CId={}, " +
                "messageId={}", clientID, messageID);
            return;
        }

        LOG.info("Creating and storing subscriptions CId={}, messageId={}, topics={}", clientID, messageID, ackTopics);

        List<Subscription> newSubscriptions = doStoreSubscription(ackTopics, clientID);

        // save session, persist subscriptions from session
        for (Subscription subscription : newSubscriptions) {
            subscriptions.add(subscription.asClientTopicCouple());
            subscriptionStore.addNewSubscription(subscription);
        }

        LOG.info("Sending SUBACK response CId={}, messageId={}", clientID, messageID);
        channel.writeAndFlush(ackMessage);

        // fire the persisted messages in session
        for (Subscription subscription : newSubscriptions) {
            publishRetainedMessagesInSession(subscription, username);
        }

        boolean success = this.subscriptionInCourse.remove(executionKey, SubscriptionState.STORED);
        if (!success) {
            LOG.warn("Unable to perform the final subscription state update CId={}, messageId={}", clientID, messageID);
        }
    }

    private List<Subscription> doStoreSubscription(List<MqttTopicSubscription> ackTopics, String clientID) {
        ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);

        List<Subscription> newSubscriptions = new ArrayList<>();
        for (MqttTopicSubscription req : ackTopics) {
            // TODO this is SUPER UGLY
            if (req.qualityOfService() == FAILURE) {
                continue;
            }
            Subscription newSubscription =
                    new Subscription(clientID, new Topic(req.topicName()), req.qualityOfService());

            clientSession.subscribe(newSubscription);
            newSubscriptions.add(newSubscription);
        }
        return newSubscriptions;
    }

    /**
     * @param clientID
     *            the clientID
     * @param username
     *            the username
     * @param msg
     *            the subscribe message to verify
     * @return the list of verified topics for the given subscribe message.
     */
    private List<MqttTopicSubscription> doVerify(String clientID, String username, MqttSubscribeMessage msg) {
        ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);
        List<MqttTopicSubscription> ackTopics = new ArrayList<>();

        final int messageId = Utils.messageId(msg);
        for (MqttTopicSubscription req : msg.payload().topicSubscriptions()) {
            Topic topic = new Topic(req.topicName());
            if ((false)) {
                // send SUBACK with 0x80, the user hasn't credentials to read the topic
                LOG.error("Client does not have read permissions on the topic CId={}, username={}, messageId={}, " +
                    "topic={}", clientID, username, messageId, topic);
                ackTopics.add(new MqttTopicSubscription(topic.toString(), FAILURE));
            } else {
                MqttQoS qos;
                if (topic.isValid()) {
                    LOG.info("Client will be subscribed to the topic CId={}, username={}, messageId={}, topic={}",
                        clientID, username, messageId, topic);
                    qos = req.qualityOfService();
                } else {
                    LOG.error("Topic filter is not valid CId={}, username={}, messageId={}, topic={}", clientID,
                        username, messageId, topic);
                    qos = FAILURE;
                }
                ackTopics.add(new MqttTopicSubscription(topic.toString(), qos));
            }
        }
        return ackTopics;
    }

    /**
     * Create the SUBACK response from a list of topicFilters
     */
    private MqttSubAckMessage doAckMessageFromValidateFilters(List<MqttTopicSubscription> topicFilters, int messageId) {
        List<Integer> grantedQoSLevels = new ArrayList<>();
        for (MqttTopicSubscription req : topicFilters) {
            grantedQoSLevels.add(req.qualityOfService().value());
        }

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, AT_LEAST_ONCE, false, 0);
        MqttSubAckPayload payload = new MqttSubAckPayload(grantedQoSLevels);
        return new MqttSubAckMessage(fixedHeader, from(messageId), payload);
    }

    private void publishRetainedMessagesInSession(final Subscription newSubscription, String username) {
        LOG.info("Retrieving retained messages CId={}, topics={}", newSubscription.getClientId(),
                newSubscription.getTopicFilter());

        // scans retained messages to be published to the new subscription
        // TODO this is ugly, it does a linear scan on potential big dataset
        Collection<IMessagesStore.StoredMessage> messages = m_messagesStore.searchMatching(new IMatchingCondition() {

            @Override
            public boolean match(Topic key) {
                return key.match(newSubscription.getTopicFilter());
            }
        });

        if (!messages.isEmpty()) {
            LOG.info("Publishing retained messages CId={}, topics={}, messagesNo={}",
                newSubscription.getClientId(), newSubscription.getTopicFilter(), messages.size());
        }
        ClientSession targetSession = m_sessionsStore.sessionForClient(newSubscription.getClientId());
        this.internalRepublisher.publishRetained(targetSession, messages);

        // notify the Observables
        m_interceptor.notifyTopicSubscribed(newSubscription, username);
    }

    public void notifyChannelWritable(Channel channel) {
        String clientID = NettyUtils.clientID(channel);
        ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);
        boolean emptyQueue = false;
        while (channel.isWritable() && !emptyQueue) {
            IMessagesStore.StoredMessage msg = clientSession.queue().poll();
            if (msg == null) {
                emptyQueue = true;
            } else {
                // recreate a publish from stored publish in queue
                MqttPublishMessage pubMsg = InternalRepublisher.createPublishForQos( msg.getTopic(), msg.getQos(), msg.getPayload(),
                        msg.isRetained(), 0);
                channel.write(pubMsg);
            }
        }
        channel.flush();
    }

    public void addInterceptHandler(InterceptHandler interceptHandler) {
        this.m_interceptor.addInterceptHandler(interceptHandler);
    }

    public void removeInterceptHandler(InterceptHandler interceptHandler) {
        this.m_interceptor.removeInterceptHandler(interceptHandler);
    }

    public IMessagesStore getMessagesStore() {
        return m_messagesStore;
    }

    public ISessionsStore getSessionsStore() {
        return m_sessionsStore;
    }



    public void reUnKnowPubRelMessage(String pubRelKey){
        PubRelEvent pubEvent = m_messagesStore.searchPubRelMessage(pubRelKey);
        LOG.info("重发PubRelKey为{"+ pubRelKey +"}的PubRel离线消息");
        sendPubRel(pubEvent.getClientID(), pubEvent.getPackgeID());
//	    messagesStore.removeQosPublishMessage(pubRelKey);
    }


    /**
     *回写PubRel消息给发来publish的客户端
     * @param clientID
     * @param packageID
     * @author zer0
     * @version 1.0
     * @date 2015-5-23
     */
    private void sendPubRel(String clientID, Integer packageID) {
        LOG.trace("发送PubRel消息给客户端");

//        Message pubRelMessage = MQTTMesageFactory.newMessage(
//                FixedHeader.getPubAckFixedHeader(),
//                new PackageIdVariableHeader(packageID),
//                null);
//
//        try {
//            if (this.dropStoredMessages(clientID) == null) {
//                throw new RuntimeException("内部错误，clients为null");
//            } else {
//                Log.debug("clients为{"+this.clients+"}");
//            }
//
//            if (this.clients.get(clientID) == null) {
//                throw new RuntimeException("不能从会话列表{"+this.clients+"}中找到clientID:{"+clientID+"}");
//            } else {
//                Log.debug("从会话列表{"+this.clients+"}查找到clientID:{"+clientID+"}");
//            }
//
//            this.clients.get(clientID).getClient().writeAndFlush(pubRelMessage);
//        }catch(Throwable t) {
//            LOG.error(null, t);
//        }
    }






    /**
     * 在客户端重连以后，针对QoS1和Qos2的消息，重发存储的离线消息
     * @param clientID
     * @author zer0
     * @version 1.0
     * @date 2015-05-18
     */
    private void republishMessage(String clientID){
        //取出需要重发的消息列表
        //查看消息列表是否为空，为空则返回
        //不为空则依次发送消息并从会话中删除此消息
        List<PublishEvent> publishedEvents = m_messagesStore.listMessagesInSession(clientID);
        if (publishedEvents.isEmpty()) {
            LOG.info("没有客户端{"+clientID+"}存储的离线消息");
            return;
        }

        LOG.info("重发客户端{"+ clientID +"}存储的离线消息");
        for (PublishEvent pubEvent : publishedEvents) {
//            boolean dup = true;
//            sendPublishMessage(pubEvent.getClientID(),
//                    pubEvent.getTopic(),
//                    pubEvent.getQos(),
//                    Unpooled.buffer().writeBytes(pubEvent.getMessage()),
//                    pubEvent.isRetain(),
//                    pubEvent.getPackgeID(),
//                    dup);
            String str="";
            try {
                str=new String(pubEvent.getMessage(),"UTF-8");
                LOG.info("重发客户端存储的离线消息:"+str);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            MqttFixedHeader fixedHeader=new MqttFixedHeader(MqttMessageType.PUBLISH,false,MqttQoS.AT_LEAST_ONCE,false,0);
            MqttPublishVariableHeader publishVariableHeader=new MqttPublishVariableHeader(pubEvent.getTopic(),pubEvent.getPackgeID());
            MqttPublishMessage publishMessage=new MqttPublishMessage(fixedHeader,publishVariableHeader, Unpooled.buffer().writeBytes(pubEvent.getMessage()));
            this.connectionDescriptors.sendMessage(publishMessage,pubEvent.getPackgeID(),pubEvent.getTopic());
            m_messagesStore.removeMessageInSessionForPublish(clientID, pubEvent.getPackgeID());
        }
    }

}
