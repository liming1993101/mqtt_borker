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

package com.sh.wd.spi.impl.security;


import com.sh.wd.bean.MessageLogEntity;
import com.sh.wd.mapper.UserMapper;
import com.sh.wd.spi.security.DBService;
import com.sh.wd.utils.DBUtils;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Load user credentials from a SQL database. sql driver must be provided at runtime
 */
public class DBServiceImpl implements DBService {

    private static final Logger LOG = LoggerFactory.getLogger(DBServiceImpl.class);



    public DBServiceImpl() {

        DBUtils.openSqlSession();

    }


    @Override
    public synchronized boolean checkValid(String clientId, String username, byte[] password) {
        return false;
    }

    @Override
    public int login(String account, String password) {
        int b=-1;
        SqlSession sqlSession = null;
        try {
            sqlSession = DBUtils.openSqlSession();
            UserMapper mapper = sqlSession.getMapper(UserMapper.class);
            b= mapper.userLogin(account,password);
            sqlSession.commit();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            sqlSession.rollback();
        } finally {
            if (sqlSession != null) {
                sqlSession.close();
            }
        }
        return b;
    }

    @Override
    public boolean updataMessageState(int messageId, int state) {
        boolean b=false;
//        b= userMapper.updateMessageState(messageId,state);
        return b;
    }

    @Override
    public void updateUserState(String account, int state) {

//
//        userMapper.updataUserState(account, state);
    }

    @Override
    public boolean addUserState(String account, int state) {
        boolean b=false;
        SqlSession sqlSession = null;
        try {
            sqlSession = DBUtils.openSqlSession();
            UserMapper mapper = sqlSession.getMapper(UserMapper.class);
            b= mapper.addUserState(account,new Date().toString(),state);
            sqlSession.commit();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            sqlSession.rollback();
        } finally {
            if (sqlSession != null) {
                sqlSession.close();
            }
        }
        return b;
    }

    @Override
    public int storeMessage(MessageLogEntity entity) {

        int messageID=-1;
        SqlSession sqlSession = null;

        try {
            sqlSession = DBUtils.openSqlSession();
            UserMapper mapper = sqlSession.getMapper(UserMapper.class);
            messageID = mapper.insertMessageLog(entity);
            sqlSession.commit();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            sqlSession.rollback();
        } finally {
            if (sqlSession != null) {
                sqlSession.close();
            }
        }
        return messageID;

    }
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }
}
