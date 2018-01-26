package com.sh.wd.redis;


import com.sh.wd.bean.PubRelEvent;
import com.sh.wd.bean.PublishEvent;
import com.sh.wd.spi.IMatchingCondition;
import com.sh.wd.spi.IMessagesStore;
import com.sh.wd.spi.impl.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class RedisMessagesStore implements IMessagesStore {

    private static final Logger LOG = LoggerFactory.getLogger(RedisMessagesStore.class);

    private RedisDao<?> redisDao;

    private final String m_retainedStore = "retainedStore_";
    private final String m_offlineStore="offlineStore_";
    private final String m_publishTemp="publishTemp_";
    private final String m_publishPID="publishPID_";
    private final String m_pubRecPID="pubRecPID_";

    public RedisMessagesStore(RedisDao<?> redisDao) {
        this.redisDao = redisDao;
    }

    @Override
    public void initStore() {
    }

    @Override
    public List<PublishEvent> listMessagesInSession(String clientID) {

        if (LOG.isDebugEnabled())
            LOG.debug("Scanning retained messages");
        List<PublishEvent> results = new ArrayList<>();
        ListOperations<String,PublishEvent> operation = (ListOperations<String, PublishEvent>) redisDao.opsForList();
        results=operation.range(m_offlineStore+clientID,0,1000);
        return results;
    }

    @Override
    public void removeMessageInSessionForPublish(String clientID, Integer packgeID) {

        List<PublishEvent>publishEvents=new ArrayList<>();
        ListOperations<String,PublishEvent> operation = (ListOperations<String, PublishEvent>) redisDao.opsForList();
        publishEvents=operation.range(m_offlineStore+clientID,0,operation.size(m_offlineStore+clientID));
        PublishEvent publishEvent=null;
        if (publishEvents.size()!=0) {
            for (PublishEvent publish : publishEvents) {
                  if (publish.getPackgeID()==packgeID){
                      publishEvent=publish;
                      break;
                  }
            }
           long number=operation.remove(m_offlineStore+clientID,1,publishEvent);
        }
    }

    @Override
    public void storeMessageToSessionForPublish(PublishEvent pubEvent) {
        String clientId=pubEvent.getClientID();
        ListOperations<String,PublishEvent> operation = (ListOperations<String, PublishEvent>) redisDao.opsForList();
        operation.rightPush(m_offlineStore+clientId,pubEvent);

    }

    @Override
    public void storePublicPackgeID(String clientID, Integer packgeID) {
       redisDao.opsForHash().put(m_publishPID,clientID,packgeID);
    }

    @Override
    public void removePublicPackgeID(String clientID) {
        redisDao.opsForHash().delete(m_publishPID,clientID);
    }

    @Override
    public void removePubRecPackgeID(String clientID) {
        redisDao.opsForHash().delete(m_pubRecPID,clientID);
    }

    @Override
    public void storePubRecPackgeID(String clientID, Integer packgeID) {
        redisDao.opsForHash().put(m_pubRecPID,clientID,packgeID);

    }

    @Override
    public void storeQosPublishMessage(String publishKey, PublishEvent pubEvent) {
        HashOperations<String,String,PublishEvent>operations=redisDao.opsForHash();
        operations.put(m_publishTemp,publishKey,pubEvent);
    }

    @Override
    public void removeQosPublishMessage(String publishKey) {
        HashOperations<String,String,PublishEvent>operations=redisDao.opsForHash();
        operations.delete(m_publishTemp,publishKey);
    }

    @Override
    public PublishEvent searchQosPublishMessage(String publishKey) {
        return (PublishEvent) redisDao.opsForHash().get(m_publishTemp,publishKey);
    }

    @Override
    public void storePubRelMessage(String pubRelKey, PubRelEvent pubRelEvent) {
        HashOperations<String,String,PubRelEvent>operations=redisDao.opsForHash();
        operations.put(m_pubRecPID,pubRelKey,pubRelEvent);

    }

    @Override
    public void removePubRelMessage(String pubRelKey) {
        HashOperations<String,String,PubRelEvent>operations=redisDao.opsForHash();
        operations.delete(m_pubRecPID,pubRelKey);
    }

    @Override
    public PubRelEvent searchPubRelMessage(String pubRelKey) {
        return (PubRelEvent) redisDao.opsForHash().get(m_pubRecPID,pubRelKey);
    }

    @Override
    public void storeRetained(String topic, ByteBuf message, MqttQoS qos) {

    }

//    @Override
//    public void storeRetained(String topic, ByteBuf message, MqttQoS qos) {
//
//    }

    @Override
    public void cleanRetained(String topic) {

    }

    @Override
    public Collection<StoredMessage> searchRetained(String topic) {
        return null;
    }

    @Override
    public Collection<StoredMessage> searchMatching(IMatchingCondition condition) {

        if (LOG.isDebugEnabled())
            LOG.debug("Scanning retained messages");

        List<StoredMessage> results = new ArrayList<>();

        HashOperations<String, Topic, StoredMessage> operation = redisDao.opsForHash();
        Set<Topic> topics = operation.keys(m_retainedStore);

        for (Topic topic : topics) {

            if (condition.match(topic)) {
                StoredMessage storedMsg = operation.get(m_retainedStore, topic);
                results.add(storedMsg);
            }
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Retained messages have been scanned matchingMessages={}", results);
        }

        return results;

    }

    @Override
    public void cleanRetained(Topic topic) {
        HashOperations<String, Topic, StoredMessage> operation = redisDao.opsForHash();

        operation.delete(m_retainedStore, topic);
    }

    @Override
    public void storeRetained(Topic topic, StoredMessage storedMessage) {
        if (LOG.isDebugEnabled())
            LOG.debug("Store retained message for topic={}, CId={}", topic, storedMessage.getClientID());

        if (storedMessage.getClientID() == null) {
            throw new IllegalArgumentException("Message to be persisted must have a not null client ID");
        }

        HashOperations<String, Topic, StoredMessage> operation = redisDao.opsForHash();
        operation.put(m_retainedStore, topic, storedMessage);
    }



}
