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

package com.sh.wd.interception.messages;

import com.sh.wd.spi.IMessagesStore;

public class InterceptAcknowledgedMessage implements InterceptMessage {

    private final IMessagesStore.StoredMessage msg;
    private final String username;
    private final String topic;
    private final int packetID;

    public InterceptAcknowledgedMessage(IMessagesStore.StoredMessage msg, String topic, String username, int packetID) {
        this.msg = msg;
        this.username = username;
        this.topic = topic;
        this.packetID = packetID;
    }

    public IMessagesStore.StoredMessage getMsg() {
        return msg;
    }

    public String getUsername() {
        return username;
    }

    public String getTopic() {
        return topic;
    }

    public int getPacketID() {
        return packetID;
    }
}
