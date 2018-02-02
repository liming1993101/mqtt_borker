package com.sh.wd.server.netty;

import com.sh.wd.server.ConnectionDescriptorStore;
import com.sh.wd.spi.security.DBService;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * Created by admin on 2017/10/23.
 */
public class TimeOutHandle extends IdleStateHandler {

    private String account;
    private DBService m_authenticator;
    private ConnectionDescriptorStore connectionDescriptorStore;
    public TimeOutHandle(int readerIdleTimeSeconds, int writerIdleTimeSeconds, int allIdleTimeSeconds, String account, DBService m_authenticator, ConnectionDescriptorStore connectionDescriptorStore) {
        super(readerIdleTimeSeconds, writerIdleTimeSeconds, allIdleTimeSeconds);
        this.account=account;
        this.m_authenticator=m_authenticator;
        this.connectionDescriptorStore=connectionDescriptorStore;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
//        ctx.channel().remoteAddress().toString();
//        System.out.print("没有再次收到心跳,客服端链接被关闭");
//        m_authenticator.addUserState(account,2);
        connectionDescriptorStore.removeConnection(connectionDescriptorStore.getConnection(account));

//        ProtocolProcess.getInstance().removeCustomer(account);

    }
}
