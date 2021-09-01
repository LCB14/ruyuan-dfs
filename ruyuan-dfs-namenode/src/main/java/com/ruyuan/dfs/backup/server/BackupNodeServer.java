package com.ruyuan.dfs.backup.server;

import com.ruyuan.dfs.backup.config.BackupNodeConfig;
import com.ruyuan.dfs.common.network.NetServer;
import com.ruyuan.dfs.common.utils.DefaultScheduler;

import java.util.Collections;

/**
 * BackupNode 的服务端
 *
 * @author Sun Dasheng
 */
public class BackupNodeServer {

    private BackupNodeConfig backupNodeConfig;
    private NetServer netServer;

    public BackupNodeServer(DefaultScheduler defaultScheduler, BackupNodeConfig backupNodeConfig) {
        this.netServer = new NetServer("BackupNode-Server", defaultScheduler);
        this.backupNodeConfig = backupNodeConfig;
    }

    /**
     * 启动并绑定端口
     *
     * @throws InterruptedException 中断异常
     */
    public void start() throws InterruptedException {
        netServer.addHandlers(Collections.singletonList(new AwareConnectHandler()));
        netServer.bind(backupNodeConfig.getBackupNodePort());
    }

    public void shutdown() {
        this.netServer.shutdown();
    }
}
