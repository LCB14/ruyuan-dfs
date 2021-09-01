package com.ruyuan.dfs.datanode.server;

import com.ruyuan.dfs.common.metrics.MetricsHandler;
import com.ruyuan.dfs.common.network.BaseChannelInitializer;
import com.ruyuan.dfs.datanode.config.DataNodeConfig;
import com.ruyuan.dfs.datanode.server.http.HttpFileServerHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

/**
 * 绑定多端口的渠道处理器
 *
 * @author Sun Dasheng
 */
public class MultiPortChannelInitializer extends BaseChannelInitializer {

    private StorageManager storageManager;
    private DataNodeConfig dataNodeConfig;

    public MultiPortChannelInitializer(DataNodeConfig dataNodeConfig, StorageManager storageManager) {
        this.dataNodeConfig = dataNodeConfig;
        this.storageManager = storageManager;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        int localPort = ch.localAddress().getPort();
        if (localPort == dataNodeConfig.getDataNodeTransportPort()) {
            super.initChannel(ch);
        } else if (localPort == dataNodeConfig.getDataNodeHttpPort()) {
            ch.pipeline().addLast(new HttpServerCodec());
            ch.pipeline().addLast(new HttpObjectAggregator(65536));
            ch.pipeline().addLast(new ChunkedWriteHandler());
            ch.pipeline().addLast(new HttpFileServerHandler(storageManager));
        }
    }
}
