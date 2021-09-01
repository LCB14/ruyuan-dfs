package com.ruyuan.dfs.datanode.config;

import lombok.Builder;
import lombok.Data;

import java.util.Properties;

/**
 * DataNode的配置
 *
 * @author Sun Dasheng
 */
@Data
@Builder
public class DataNodeConfig {

    private String baseDir;
    private String nameNodeServers;
    private String dataNodeTransportServer;
    private String dataNodeHttpServer;

    private int heartbeatInterval;
    private int dataNodeId;
    private String fileLocatorType;
    private int dataNodeWorkerThreads;

    public static DataNodeConfig parse(Properties properties) {
        String baseDir = (String) properties.get("base.dir");
        String nameNodeServers = (String) properties.get("namenode.servers");
        String dataNodeTransportServer = (String) properties.get("datanode.transport.server");
        String dataNodeHttpServer = (String) properties.get("datanode.http.server");
        int heartbeatInterval = Integer.parseInt((String) properties.get("datanode.heartbeat.interval"));
        int dataNodeId = Integer.parseInt((String) properties.get("datanode.id"));
        String fileLocatorType = (String) properties.get("file.locator.type");
        int dataNodeWorkerThreads = Integer.parseInt((String) properties.get("datanode.worker.threads"));
        return DataNodeConfig.builder()
                .baseDir(baseDir)
                .nameNodeServers(nameNodeServers)
                .dataNodeTransportServer(dataNodeTransportServer)
                .dataNodeHttpServer(dataNodeHttpServer)
                .heartbeatInterval(heartbeatInterval)
                .dataNodeId(dataNodeId)
                .fileLocatorType(fileLocatorType)
                .dataNodeWorkerThreads(dataNodeWorkerThreads)
                .build();
    }


    public int getNameNodePort() {
        return Integer.parseInt(nameNodeServers.split(":")[1]);
    }

    public int getDataNodeHttpPort() {
        return Integer.parseInt(dataNodeHttpServer.split(":")[1]);
    }


    public String getNameNodeAddr() {
        return nameNodeServers.split(":")[0];
    }

    public int getDataNodeTransportPort() {
        return Integer.parseInt(dataNodeTransportServer.split(":")[1]);
    }

    public String getDataNodeTransportAddr() {
        return dataNodeTransportServer.split(":")[0];
    }
}
