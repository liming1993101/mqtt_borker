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
import com.sh.wd.spi.security.DBService;

public class AcceptAllAuthenticator implements DBService {

    public boolean checkValid(String clientId, String username, byte[] password) {
        return true;
    }

    @Override
    public int login(String clientId, String password) {
        return 0;
    }

    @Override
    public boolean updataMessageState(int messageId, int state) {
        return false;
    }

    @Override
    public void updateUserState(String account, int state) {

    }

    @Override
    public boolean addUserState(String account, int state) {
        return false;
    }

    @Override
    public int storeMessage(MessageLogEntity entity) {
        return 0;
    }
}
