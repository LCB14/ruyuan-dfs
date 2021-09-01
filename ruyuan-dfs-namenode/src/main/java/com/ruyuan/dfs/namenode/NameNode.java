package com.ruyuan.dfs.namenode;


import com.ruyuan.dfs.common.metrics.Prometheus;
import com.ruyuan.dfs.common.utils.DefaultScheduler;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.datanode.DataNodeManager;
import com.ruyuan.dfs.namenode.fs.DiskNameSystem;
import com.ruyuan.dfs.namenode.server.NameNodeApis;
import com.ruyuan.dfs.namenode.server.NameNodeServer;
import com.ruyuan.dfs.namenode.server.UserManager;
import com.ruyuan.dfs.namenode.server.tomcat.TomcatServer;
import com.ruyuan.dfs.namenode.shard.ShardingManager;
import com.ruyuan.dfs.namenode.shard.controller.ControllerManager;
import com.ruyuan.dfs.namenode.shard.peer.PeerNameNodes;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NameNode核心管理组件
 *
 * @author Sun Dasheng
 */
@Slf4j
public class NameNode {
    private final NameNodeApis nameNodeApis;
    private final ControllerManager controllerManager;
    private final UserManager userManager;
    private final TomcatServer tomcatServer;
    private final DefaultScheduler defaultScheduler;
    private final DataNodeManager dataNodeManager;
    private final ShardingManager shardingManager;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final DiskNameSystem diskNameSystem;
    private final NameNodeServer nameNodeServer;
    private final PeerNameNodes peerNameNodes;

    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("配置文件不能为空");
        }
        // 1. 解析配置文件
        NameNodeConfig nameNodeConfig = null;
        try {
            Path path = Paths.get(args[0]);
            try (InputStream inputStream = Files.newInputStream(path)) {
                Properties properties = new Properties();
                properties.load(inputStream);
                nameNodeConfig = NameNodeConfig.parse(properties);
            }
            log.info("NameNode启动配置文件 : {}", path.toAbsolutePath());
        } catch (Exception e) {
            log.error("无法加载配置文件 : ", e);
            System.exit(1);
        }
        parseOption(args, nameNodeConfig);
        try {
            NameNode namenode = new NameNode(nameNodeConfig);
            Runtime.getRuntime().addShutdownHook(new Thread(namenode::shutdown));
            namenode.start();
        } catch (Exception e) {
            log.error("启动NameNode失败：", e);
            System.exit(1);
        }
    }

    private static void parseOption(String[] args, NameNodeConfig nameNodeConfig) {
        OptionParser parser = new OptionParser();
        OptionSpec<String> baseDirOpt = parser.accepts("baseDir").withOptionalArg().ofType(String.class);
        OptionSpec<Integer> nameNodeIdOpt = parser.accepts("nameNodeId").withOptionalArg().ofType(Integer.class);
        OptionSpec<Integer> portOpt = parser.accepts("port").withOptionalArg().ofType(Integer.class);
        OptionSpec<Integer> httpPortOpt = parser.accepts("httpPort").withOptionalArg().ofType(Integer.class);
        OptionSpec<String> launchModeOpt = parser.accepts("launchMode").withOptionalArg().ofType(String.class);
        OptionSpec<String> peerServersOpt = parser.accepts("peerServers").withOptionalArg().ofType(String.class);
        OptionSpec<Integer> coreSizeOpt = parser.accepts("coreSize").withOptionalArg().ofType(Integer.class);
        OptionSpec<Integer> maximumPoolSizeOpt = parser.accepts("maximumPoolSize").withOptionalArg().ofType(Integer.class);
        OptionSpec<Integer> queueSizeOpt = parser.accepts("queueSize").withOptionalArg().ofType(Integer.class);
        parser.allowsUnrecognizedOptions();
        OptionSet parse = parser.parse(args);
        if (parse.has(baseDirOpt)) {
            String baseDir = parse.valueOf(baseDirOpt);
            nameNodeConfig.setBaseDir(baseDir);
            log.info("从参数读取到配置进行替换：[key={}, value={}]", "baseDir", baseDir);
        }
        if (parse.has(nameNodeIdOpt)) {
            Integer nameNodeId = parse.valueOf(nameNodeIdOpt);
            nameNodeConfig.setNameNodeId(nameNodeId);
            log.info("从参数读取到配置进行替换：[key={}, value={}]", "nameNodeId", nameNodeId);
        }
        if (parse.has(portOpt)) {
            Integer port = parse.valueOf(portOpt);
            nameNodeConfig.setPort(port);
            log.info("从参数读取到配置进行替换：[key={}, value={}]", "port", port);
        }
        if (parse.has(httpPortOpt)) {
            Integer httpPort = parse.valueOf(httpPortOpt);
            nameNodeConfig.setHttpPort(httpPort);
            log.info("从参数读取到配置进行替换：[key={}, value={}]", "httpPort", httpPort);
        }
        if (parse.has(launchModeOpt)) {
            String launchMode = parse.valueOf(launchModeOpt);
            nameNodeConfig.setNameNodeLaunchMode(launchMode);
            log.info("从参数读取到配置进行替换：[key={}, value={}]", "nameNodeLaunchMode", launchMode);
        }
        if (parse.has(peerServersOpt)) {
            String peerServers = parse.valueOf(peerServersOpt);
            nameNodeConfig.setNameNodePeerServers(peerServers);
            log.info("从参数读取到配置进行替换：[key={}, value={}]", "peerServers", peerServers);
        }
        if (parse.has(coreSizeOpt)) {
            Integer coreSize = parse.valueOf(coreSizeOpt);
            nameNodeConfig.setNameNodeApiCoreSize(coreSize);
            log.info("从参数读取到配置进行替换：[key={}, value={}]", "coreSize", coreSize);
        }
        if (parse.has(maximumPoolSizeOpt)) {
            Integer maximumPoolSize = parse.valueOf(maximumPoolSizeOpt);
            nameNodeConfig.setNameNodeApiMaximumPoolSize(maximumPoolSize);
            log.info("从参数读取到配置进行替换：[key={}, value={}]", "maximumPoolSize", maximumPoolSize);
        }
        if (parse.has(queueSizeOpt)) {
            Integer queueSize = parse.valueOf(queueSizeOpt);
            nameNodeConfig.setNameNodeApiQueueSize(queueSize);
            log.info("从参数读取到配置进行替换：[key={}, value={}]", "queueSize", queueSize);
        }
    }

    public NameNode(NameNodeConfig nameNodeConfig) {
        this.defaultScheduler = new DefaultScheduler("NameNode-Scheduler-");
        this.userManager = new UserManager(nameNodeConfig, defaultScheduler);
        this.dataNodeManager = new DataNodeManager(nameNodeConfig, defaultScheduler, userManager);
        this.diskNameSystem = new DiskNameSystem(nameNodeConfig, defaultScheduler, dataNodeManager, userManager);
        this.peerNameNodes = new PeerNameNodes(defaultScheduler, nameNodeConfig);
        this.controllerManager = new ControllerManager(nameNodeConfig, peerNameNodes, defaultScheduler, diskNameSystem,
                dataNodeManager, userManager);
        this.shardingManager = new ShardingManager(nameNodeConfig, peerNameNodes, controllerManager);
        this.nameNodeApis = new NameNodeApis(diskNameSystem.getNameNodeConfig(), dataNodeManager, peerNameNodes,
                shardingManager, diskNameSystem, defaultScheduler, userManager, controllerManager);
        this.nameNodeServer = new NameNodeServer(defaultScheduler, diskNameSystem, nameNodeApis);
        this.tomcatServer = new TomcatServer(nameNodeConfig, dataNodeManager, peerNameNodes, shardingManager,
                userManager, diskNameSystem, nameNodeApis);
    }

    /**
     * 启动
     *
     * @throws Exception 中断异常
     */
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            this.diskNameSystem.recoveryNamespace();
            this.shardingManager.start();
            this.tomcatServer.start();
            this.nameNodeServer.start();
        }
    }

    /**
     * 优雅停机
     */
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            this.defaultScheduler.shutdown();
            this.diskNameSystem.shutdown();
            this.tomcatServer.shutdown();
            this.peerNameNodes.shutdown();
            this.nameNodeServer.shutdown();
        }
    }
}
