package com.ruyuan.dfs.backup;

import com.ruyuan.dfs.backup.config.BackupNodeConfig;
import com.ruyuan.dfs.backup.fs.InMemoryNameSystem;
import com.ruyuan.dfs.backup.fs.NameNodeClient;
import com.ruyuan.dfs.backup.server.BackupNodeServer;
import com.ruyuan.dfs.common.utils.DefaultScheduler;
import com.ruyuan.dfs.ha.NodeRoleSwitcher;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
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
 * NameNode 备份节点
 *
 * @author Sun Dasheng
 */
@Slf4j
public class BackupNode {

    private final DefaultScheduler defaultScheduler;
    private final InMemoryNameSystem nameSystem;
    private final NameNodeClient nameNodeClient;
    private final BackupNodeServer backupNodeServer;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("Missing config file path.");
        }
        BackupNodeConfig backupNodeConfig = null;
        try {
            Path path = Paths.get(args[0]);
            try (InputStream inputStream = Files.newInputStream(path)) {
                Properties properties = new Properties();
                properties.load(inputStream);
                backupNodeConfig = BackupNodeConfig.parse(properties);
            }
            log.info("BackupNode加载配置文件: [file={}].", path.toAbsolutePath().toString());
        } catch (Exception e) {
            log.error("无法加载配置文件 : ", e);
            System.exit(0);
        }
        parseOption(args, backupNodeConfig);
        try {
            BackupNode backupNode = new BackupNode(backupNodeConfig);
            NodeRoleSwitcher.getInstance().setBackupNode(backupNode);
            backupNode.start();
            Runtime.getRuntime().addShutdownHook(new Thread(backupNode::shutdown));
        } catch (Exception e) {
            log.error("启动BackupNode失败：", e);
            System.exit(1);
        }
    }

    private static void parseOption(String[] args, BackupNodeConfig backupNodeConfig) {
        OptionParser parser = new OptionParser();
        OptionSpec<String> baseDirOpt = parser.accepts("baseDir").withOptionalArg().ofType(String.class);
        OptionSpec<String> nameNodeServerOpt = parser.accepts("nameNodeServer").withOptionalArg().ofType(String.class);
        OptionSpec<String> backupNodeServerOpt = parser.accepts("backupNodeServer").withOptionalArg().ofType(String.class);
        OptionSet parse = parser.parse(args);
        if (parse.hasArgument(baseDirOpt)) {
            String baseDir = parse.valueOf(baseDirOpt);
            backupNodeConfig.setBaseDir(baseDir);
            log.info("从参数读取到配置进行替换：[key={}, value={}]", "baseDir", baseDir);
        }
        if (parse.hasArgument(nameNodeServerOpt)) {
            String nameNodeServer = parse.valueOf(nameNodeServerOpt);
            backupNodeConfig.setNameNodeServer(nameNodeServer);
            log.info("从参数读取到配置进行替换：[key={}, value={}]", "nameNodeServer", nameNodeServer);
        }
        if (parse.hasArgument(backupNodeServerOpt)) {
            String backupNodeServer = parse.valueOf(backupNodeServerOpt);
            if (backupNodeServer != null) {
                backupNodeConfig.setBackupNodeServer(backupNodeServer);
                log.info("从参数读取到配置进行替换：[key={}, value={}]", "backupNodeServer", backupNodeServer);
            }
        }
    }


    /**
     * 启动BackupNode
     *
     * @throws Exception 中断异常
     */
    private void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            this.nameSystem.recoveryNamespace();
            this.nameNodeClient.start();
            this.backupNodeServer.start();
        }
    }

    public BackupNode(BackupNodeConfig backupNodeConfig) {
        this.defaultScheduler = new DefaultScheduler("BackupNode-Scheduler-");
        this.nameSystem = new InMemoryNameSystem(backupNodeConfig);
        this.nameNodeClient = new NameNodeClient(defaultScheduler, backupNodeConfig, nameSystem);
        this.backupNodeServer = new BackupNodeServer(defaultScheduler, backupNodeConfig);
    }

    public InMemoryNameSystem getNameSystem() {
        return this.nameSystem;
    }


    /**
     * 优雅停止
     */
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            this.defaultScheduler.shutdown();
            this.nameNodeClient.shutdown();
            this.backupNodeServer.shutdown();
        }
    }
}
