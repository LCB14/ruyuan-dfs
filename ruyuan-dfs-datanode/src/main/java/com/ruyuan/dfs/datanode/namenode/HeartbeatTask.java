package com.ruyuan.dfs.datanode.namenode;

import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.enums.PacketType;
import com.ruyuan.dfs.datanode.config.DataNodeConfig;
import com.ruyuan.dfs.model.datanode.HeartbeatRequest;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * 往NameNode发送心跳的请求
 *
 * @author Sun Dasheng
 */
@Slf4j
public class HeartbeatTask implements Runnable {
    private DataNodeConfig datanodeConfig;
    private ChannelHandlerContext ctx;

    public HeartbeatTask(ChannelHandlerContext ctx, DataNodeConfig datanodeConfig) {
        this.ctx = ctx;
        this.datanodeConfig = datanodeConfig;
    }

    @Override
    public void run() {
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
                .setHostname(datanodeConfig.getDataNodeTransportAddr())
                .build();
        // 发送心跳请求
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.HEART_BRET);
        ctx.writeAndFlush(nettyPacket);
    }
}