package com.ruyuan.dfs.namenode.shard.peer;

import com.ruyuan.dfs.common.Constants;
import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.exception.RequestTimeoutException;
import com.ruyuan.dfs.common.network.SyncRequestSupport;
import com.ruyuan.dfs.common.utils.DefaultScheduler;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * 表示和PeerDataNode的连接，当前NameNode作为服务端
 *
 * @author Sun Dasheng
 */
@Slf4j
public class PeerNameNodeServer extends AbstractPeerNameNode {
    private final String name;
    private volatile SocketChannel socketChannel;
    private final SyncRequestSupport syncRequestSupport;

    public PeerNameNodeServer(SocketChannel socketChannel, int currentNodeId, int targetNodeId, String server, DefaultScheduler defaultScheduler) {
        super(currentNodeId, targetNodeId, server);
        this.name = "NameNode-PeerNode-" + currentNodeId + "-" + targetNodeId;
        this.syncRequestSupport = new SyncRequestSupport(this.name, defaultScheduler, 5000);
        this.setSocketChannel(socketChannel);
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        synchronized (this) {
            this.socketChannel = socketChannel;
            this.syncRequestSupport.setSocketChannel(socketChannel);
            notifyAll();
        }
    }

    @Override
    public void send(NettyPacket nettyPacket) {
        synchronized (this) {
            // 如果这里断开连接了，会一直等待直到客户端会重新建立连接
            while (!isConnected()) {
                try {
                    wait(10);
                } catch (InterruptedException e) {
                    log.error("PeerDataNodeServer#send has Interrupted !!");
                }
            }
        }
        nettyPacket.setSequence(name + "-" + Constants.REQUEST_COUNTER.getAndIncrement());
        socketChannel.writeAndFlush(nettyPacket);
    }

    @Override
    public NettyPacket sendSync(NettyPacket nettyPacket) throws InterruptedException, RequestTimeoutException {
        return syncRequestSupport.sendRequest(nettyPacket);
    }

    /**
     * 收到消息响应
     *
     * @param request 消息响应
     */
    public boolean onMessage(NettyPacket request) {
        return syncRequestSupport.onResponse(request);
    }


    @Override
    public void close() {
        socketChannel.close();
    }

    @Override
    public boolean isConnected() {
        return socketChannel != null && socketChannel.isActive();
    }
}
