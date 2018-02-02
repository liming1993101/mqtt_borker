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


import com.sh.wd.config.IConfig;
import com.sh.wd.redis.RedisStorageService;
import com.sh.wd.spi.IMessagesStore;
import com.sh.wd.spi.ISessionsStore;
import com.sh.wd.spi.IStore;

public class MemoryStorageService implements IStore {

//    private MemorySessionStore m_sessionsStore;
//    private MemoryMessagesStore m_messagesStore;
    private ISessionsStore m_sessionsStore;
    private IMessagesStore m_messagesStore;
    public MemoryStorageService(IConfig props) {
//
        RedisStorageService redisStorageService=new RedisStorageService(props);
        m_sessionsStore=redisStorageService.sessionsStore();
        m_messagesStore=redisStorageService.messagesStore();
//        m_sessionsStore=new MemorySessionStore();
//        m_messagesStore=new MemoryMessagesStore();
//        m_messagesStore.initStore();
//        m_sessionsStore.initStore();
    }

    @Override
    public IMessagesStore messagesStore() {
        return m_messagesStore;
    }

    @Override
    public ISessionsStore sessionsStore() {
        return m_sessionsStore;
    }

    @Override
    public void initStore() {

    }

    @Override
    public void close() {
    }
}
