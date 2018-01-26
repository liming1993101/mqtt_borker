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

package com.sh.wd.spi.security;

import com.sh.wd.bean.MessageLogEntity;

/**
 * username and password checker
 */
public interface IAuthenticator {

    boolean checkValid(String clientId, String username, byte[] password);
    int login(String clientId,String password);
    /**
     * 修改消息状态
     */
    boolean updataMessageState(int messageId,int state);

    void updateUserState(String account,int state);

    /**
     * 添加用登陆离线信息
     */
    boolean addUserState(String account,int state);
    int storeMessage(MessageLogEntity entity);

}
