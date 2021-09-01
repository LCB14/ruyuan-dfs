package com.ruyuan.dfs.namenode.server;

import com.ruyuan.dfs.common.network.NetServer;
import com.ruyuan.dfs.common.utils.DefaultScheduler;
import com.ruyuan.dfs.namenode.fs.DiskNameSystem;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

/**
 * NameNode对外提供服务接口
 *
 * @author Sun Dasheng
 */
@Slf4j
public class NameNodeServer {
    private NameNodeApis nameNodeApis;
    private DiskNameSystem diskNameSystem;
    private NetServer netServer;

    public NameNodeServer(DefaultScheduler defaultScheduler, DiskNameSystem diskNameSystem, NameNodeApis nameNodeApis) {
        this.diskNameSystem = diskNameSystem;
        this.nameNodeApis = nameNodeApis;
        this.netServer = new NetServer("NameNode-Server", defaultScheduler);
    }

    /**
     * 启动一个Socket Server，监听指定的端口号
     */
    public void start() throws InterruptedException {
        this.netServer.addHandlers(Collections.singletonList(nameNodeApis));
        netServer.bind(diskNameSystem.getNameNodeConfig().getPort());
    }


    /**
     * 停止服务
     */
    public void shutdown() {
        log.info("Shutdown NameNodeServer.");
        netServer.shutdown();
    }
}
