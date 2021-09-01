package com.ruyuan.dfs.datanode.server;


import com.ruyuan.dfs.common.network.NetServer;
import com.ruyuan.dfs.common.utils.DefaultScheduler;
import com.ruyuan.dfs.datanode.config.DataNodeConfig;
import com.ruyuan.dfs.datanode.replica.PeerDataNodes;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;

/**
 * 用于上传、下载文件的Netty服务端
 *
 * @author Sun Dasheng
 */
@Slf4j
public class DataNodeServer {

    private DataNodeApis dataNodeApis;
    private StorageManager storageManager;
    private NetServer netServer;
    private DataNodeConfig dataNodeConfig;
    private PeerDataNodes peerDataNodes;

    public DataNodeServer(DataNodeConfig dataNodeConfig, DefaultScheduler defaultScheduler, StorageManager storageManager,
                          PeerDataNodes peerDataNodes, DataNodeApis dataNodeApis) {
        this.dataNodeConfig = dataNodeConfig;
        this.peerDataNodes = peerDataNodes;
        this.storageManager = storageManager;
        this.dataNodeApis = dataNodeApis;
        this.netServer = new NetServer("DataNode-Server", defaultScheduler, dataNodeConfig.getDataNodeWorkerThreads());
    }

    /**
     * 启动
     */
    public void start() throws InterruptedException {
        // 用于接收PeerDataNode发过来的通知信息
        MultiPortChannelInitializer multiPortChannelInitializer = new MultiPortChannelInitializer(dataNodeConfig, storageManager);
        multiPortChannelInitializer.addHandlers(Collections.singletonList(dataNodeApis));
        this.netServer.setChannelInitializer(multiPortChannelInitializer);
        this.netServer.bind(Arrays.asList(dataNodeConfig.getDataNodeTransportPort(), dataNodeConfig.getDataNodeHttpPort()));
    }

    /**
     * 优雅停止
     */
    public void shutdown() {
        log.info("Shutdown DataNodeServer");
        this.netServer.shutdown();
        this.peerDataNodes.shutdown();
    }

}
