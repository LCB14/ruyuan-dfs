package com.ruyuan.dfs.namenode.shard.peer;

import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.exception.RequestTimeoutException;

/**
 * 表示一个NameNode节点的连接
 *
 * @author Sun Dasheng
 */
public interface PeerNameNode {
    /**
     * 往 PeerNameNode发送网络包, 如果连接断开了，会同步等待连接重新建立
     *
     * @param nettyPacket 网络包
     * @throws InterruptedException 中断异常
     */
    void send(NettyPacket nettyPacket) throws InterruptedException;

    /**
     * 往 PeerNameNode发送网络包, 同步发送
     *
     * @param nettyPacket 网络包
     * @return 响应
     * @throws InterruptedException    中断异常
     * @throws RequestTimeoutException 请求超时
     */
    NettyPacket sendSync(NettyPacket nettyPacket) throws InterruptedException, RequestTimeoutException;

    /**
     * 关闭连接
     */
    void close();

    /**
     * 获取NameNodeId
     *
     * @return NameNode ID
     */
    int getTargetNodeId();

    /**
     * 获取服务连接的IP和端口号
     *
     * @return IP 和端口号
     */
    String getServer();

    /**
     * 是否连接上
     *
     * @return 是否连接上
     */
    boolean isConnected();
}

