package com.ruyuan.dfs.namenode.shard.peer;

import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.exception.RequestTimeoutException;
import com.ruyuan.dfs.common.network.NetClient;

/**
 * 表示和PeerNameNode的连接，当前NameNode作为客户端
 *
 * @author Sun Dasheng
 */
public class PeerNameNodeClient extends AbstractPeerNameNode {
    private NetClient netClient;


    public PeerNameNodeClient(NetClient netClient, int currentNodeId, int targetNodeId, String server) {
        super(currentNodeId, targetNodeId, server);
        this.netClient = netClient;
    }

    @Override
    public void send(NettyPacket nettyPacket) throws InterruptedException {
        netClient.send(nettyPacket);
    }

    @Override
    public NettyPacket sendSync(NettyPacket nettyPacket) throws InterruptedException, RequestTimeoutException {
        return netClient.sendSync(nettyPacket);
    }

    @Override
    public void close() {
        netClient.shutdown();
    }

    @Override
    public boolean isConnected() {
        return netClient.isConnected();
    }
}
