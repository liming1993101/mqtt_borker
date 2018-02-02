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

public class MemoryMessagesStore {
//
//    private static final Logger LOG = LoggerFactory.getLogger(MemoryMessagesStore.class);
//
//    private Map<Topic, StoredMessage> m_retainedStore = new HashMap<>();
//
//    //为Session保存的的可能需要重发的消息
//    private ConcurrentMap<String, List<PublishEvent>> persistentOfflineMessage;
//    //为Qos1和Qos2临时保存的消息
//    private ConcurrentMap<String, PublishEvent> persistentQosTempMessage;
//    //为Qos2临时保存的PubRel消息
//    private ConcurrentMap<String, PubRelEvent> persistentPubRelTempMessage;
//    //持久化存储session和与之对应的subscription Set
//    private ConcurrentMap<String, Set<Subscription>> persistentSubscriptionStore;
//    //持久化的Retain
//    private ConcurrentMap<String, StoredMessage> retainedStore;
//    //保存publish包ID
//    private ConcurrentMap<String, Integer> publishPackgeIDStore;
//    //保存pubRec包ID
//    private ConcurrentMap<String, Integer> pubRecPackgeIDStore;
//    private DB m_db;
//    MemoryMessagesStore() {
//    }
//
//    @Override
//    public void initStore() {
//
//        String STORAGE_FILE_PATH =  System.getProperty("user.dir") + File.separator + MqttTool.getProperty("path");
//        LOG.info("存储文件的初始化位置"+STORAGE_FILE_PATH);
//        File tmpFile;
//        try {
//            tmpFile = new File(STORAGE_FILE_PATH);
//            tmpFile.createNewFile();
//            tmpFile.delete();
//            tmpFile.deleteOnExit();
//            m_db = DBMaker.fileDB(tmpFile)
//                    .make();
//            persistentOfflineMessage = m_db.treeMap("offline", Serializer.STRING,Serializer.JAVA).createOrOpen();
//            persistentQosTempMessage = m_db.treeMap("publishTemp",Serializer.STRING,Serializer.JAVA).createOrOpen();
//            persistentPubRelTempMessage = m_db.treeMap("pubRelTemp",Serializer.STRING,Serializer.JAVA).createOrOpen();
//            persistentSubscriptionStore = m_db.treeMap("subscriptions",Serializer.STRING,Serializer.JAVA).createOrOpen();
//            retainedStore = m_db.treeMap("retained",Serializer.STRING,Serializer.JAVA).createOrOpen();
//            publishPackgeIDStore = m_db.treeMap("publishPID",Serializer.STRING,Serializer.JAVA).createOrOpen();
//            pubRecPackgeIDStore = m_db.treeMap("pubRecPID",Serializer.STRING,Serializer.JAVA).createOrOpen();
//
//        } catch (IOException ex) {
//            LOG.error(null, ex);
//        }
//    }
//
//    @Override
//    public void storeRetained(Topic topic, StoredMessage storedMessage) {
//        if(LOG.isDebugEnabled()){
//            LOG.debug("Store retained message for topic={}, CId={}", topic, storedMessage.getClientID());
//        }
//        if (storedMessage.getClientID() == null) {
//            throw new IllegalArgumentException( "Message to be persisted must have a not null client ID");
//        }
//        m_retainedStore.put(topic, storedMessage);
//    }
//
//
//
//    @Override
//    public Collection<StoredMessage> searchMatching(IMatchingCondition condition) {
//        if(LOG.isDebugEnabled()){
//            LOG.debug("searchMatching scanning all retained messages, presents are {}", m_retainedStore.size());
//        }
//        List<StoredMessage> results = new ArrayList<>();
//
//        for (Map.Entry<Topic, StoredMessage> entry : m_retainedStore.entrySet()) {
//            StoredMessage storedMsg = entry.getValue();
//            if (condition.match(entry.getKey())) {
//                results.add(storedMsg);
//            }
//        }
//
//        return results;
//    }
//
//    @Override
//    public void cleanRetained(Topic topic) {
//        m_retainedStore.remove(topic);
//    }
//
//
//
//    @Override
//    public List<PublishEvent> listMessagesInSession(String clientID) {
//        List<PublishEvent> allEvents = new ArrayList<PublishEvent>();
//        List<PublishEvent> storeEvents = persistentOfflineMessage.get(clientID);
//        //如果该client无离线消息，则把storeEvents设置为空集合
//        if (storeEvents == null) {
//            storeEvents = Collections.<PublishEvent>emptyList();
//        }
////        for (PublishEvent event : storeEvents) {
////            allEvents.add(event);
////        }
//        return storeEvents;
//    }
//
//    @Override
//    public void removeMessageInSessionForPublish(String clientID,
//                                                 Integer packgeID) {
//        List<PublishEvent> events = persistentOfflineMessage.get(clientID);
//        if (events == null) {
//            return;
//        }
//        PublishEvent toRemoveEvt = null;
//        for (PublishEvent evt : events) {
//            if (evt.getPackgeID()== packgeID) {
//                toRemoveEvt = evt;
//            }
//        }
//        events.remove(toRemoveEvt);
//        persistentOfflineMessage.put(clientID, events);
//        m_db.commit();
//    }
//
//    @Override
//    public void storeMessageToSessionForPublish(PublishEvent pubEvent) {
//        List<PublishEvent> storedEvents;
//        String clientID = pubEvent.getClientID();
//        if (!persistentOfflineMessage.containsKey(clientID)) {
//            storedEvents = new ArrayList<PublishEvent>();
//        } else {
//            storedEvents = persistentOfflineMessage.get(clientID);
//        }
//        storedEvents.add(pubEvent);
//        persistentOfflineMessage.put(clientID, storedEvents);
//        m_db.commit();
//    }
//
//    @Override
//    public void storeQosPublishMessage(String publishKey, PublishEvent pubEvent) {
//        persistentQosTempMessage.put(publishKey, pubEvent);
//        m_db.commit();
//    }
//
//    @Override
//    public void removeQosPublishMessage(String publishKey) {
//        persistentQosTempMessage.remove(publishKey);
//        m_db.commit();
//    }
//
//    @Override
//    public PublishEvent searchQosPublishMessage(String publishKey) {
//        return persistentQosTempMessage.get(publishKey);
//    }
//
//    @Override
//    public void storePubRelMessage(String pubRelKey, PubRelEvent pubRelEvent) {
//        persistentPubRelTempMessage.put(pubRelKey, pubRelEvent);
//        m_db.commit();
//    }
//
//    @Override
//    public void removePubRelMessage(String pubRelKey) {
//        persistentPubRelTempMessage.remove(pubRelKey);
//        m_db.commit();
//    }
//
//    @Override
//    public PubRelEvent searchPubRelMessage(String pubRelKey) {
//        return persistentPubRelTempMessage.get(pubRelKey);
//    }
//
//    @Override
//    public void storeRetained(String topic, ByteBuf message, MqttQoS qos) {
//        //将ByteBuf转变为byte[]
//        byte[] messageBytes = new byte[message.readableBytes()];
//        message.getBytes(message.readerIndex(), messageBytes);
//        if (messageBytes.length <= 0) {
//            retainedStore.remove(topic);
//        } else {
//            StoredMessage storedMessage = new StoredMessage(messageBytes, qos, topic);
//            retainedStore.put(topic, storedMessage);
//        }
//        m_db.commit();
//    }
//
//    @Override
//    public void cleanRetained(String topic) {
//        retainedStore.remove(topic);
//        m_db.commit();
//    }
//
//    @Override
//    public Collection<StoredMessage> searchRetained(String topic) {
//        List<StoredMessage> results = new ArrayList<StoredMessage>();
////        for (Map.Entry<String, StoredMessage> entry : retainedStore.entrySet()) {
////            StoredMessage storedMsg = entry.getValue();
////            if (Sus.matchTopics(entry.getKey(), topic)) {
////                results.add(storedMsg);
////            }
////        }
//        return results;
//    }
//
//    @Override
//    public boolean updataMessageState(int messageId, int state) {
//        boolean b=false;
//        SqlSession sqlSession = null;
//        try {
//            sqlSession = DBUtils.openSqlSession();
//            UserMapper mapper = sqlSession.getMapper(UserMapper.class);
//            b= mapper.updateMessageState(messageId,state);
//            sqlSession.commit();
//        } catch (Exception e) {
//            System.err.println(e.getMessage());
//            sqlSession.rollback();
//        } finally {
//            if (sqlSession != null) {
//                sqlSession.close();
//            }
//        }
//        return b;
//    }
//
//    @Override
//    public void updateUserState(String account, int state) {
//        SqlSession sqlSession = null;
//        try {
//            sqlSession = DBUtils.openSqlSession();
//            UserMapper mapper = sqlSession.getMapper(UserMapper.class);
//            mapper.updataUserState(account,state);
//            sqlSession.commit();
//        } catch (Exception e) {
//            System.err.println(e.getMessage());
//            sqlSession.rollback();
//        } finally {
//            if (sqlSession != null) {
//                sqlSession.close();
//            }
//        }
//    }
//
//    @Override
//    public boolean addUserState(String account, int state) {
//        boolean b=false;
//        SqlSession sqlSession = null;
//        try {
//            sqlSession = DBUtils.openSqlSession();
//            UserMapper mapper = sqlSession.getMapper(UserMapper.class);
//            b= mapper.addUserState(account,new Date().toString(),state);
//            sqlSession.commit();
//        } catch (Exception e) {
//            System.err.println(e.getMessage());
//            sqlSession.rollback();
//        } finally {
//            if (sqlSession != null) {
//                sqlSession.close();
//            }
//        }
//        return b;
//    }
//
//    @Override
//    public int login(String account, String password) {
//        int b=-1;
//        SqlSession sqlSession = null;
//        try {
//            sqlSession = DBUtils.openSqlSession();
//            UserMapper mapper = sqlSession.getMapper(UserMapper.class);
//            b= mapper.userLogin(account,password);
//            sqlSession.commit();
//        } catch (Exception e) {
//            System.err.println(e.getMessage());
//            sqlSession.rollback();
//        } finally {
//            if (sqlSession != null) {
//                sqlSession.close();
//            }
//        }
//        return b;
//    }
//
//    @Override
//    public void storePublicPackgeID(String clientID, Integer packgeID) {
//        publishPackgeIDStore.put(clientID, packgeID);
//        m_db.commit();
//    }
//
//    @Override
//    public void removePublicPackgeID(String clientID) {
//        publishPackgeIDStore.remove(clientID);
//        m_db.commit();
//    }
//
//    @Override
//    public void storePubRecPackgeID(String clientID, Integer packgeID) {
//        pubRecPackgeIDStore.put(clientID, packgeID);
//        m_db.commit();
//    }
//
//    @Override
//    public void removePubRecPackgeID(String clientID) {
//        pubRecPackgeIDStore.remove(clientID);
//        m_db.commit();
//    }
}
