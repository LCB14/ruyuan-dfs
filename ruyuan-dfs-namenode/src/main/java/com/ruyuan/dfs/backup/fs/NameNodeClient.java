package com.ruyuan.dfs.backup.fs;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ruyuan.dfs.backup.config.BackupNodeConfig;
import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.enums.PacketType;
import com.ruyuan.dfs.common.exception.RequestTimeoutException;
import com.ruyuan.dfs.common.network.NetClient;
import com.ruyuan.dfs.common.utils.DefaultScheduler;
import com.ruyuan.dfs.ha.NodeRoleSwitcher;
import com.ruyuan.dfs.model.backup.*;
import com.ruyuan.dfs.model.namenode.UserEntity;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.server.tomcat.domain.User;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 负责和nameNode通讯的客户端
 *
 * @author Sun Dasheng
 */
@Slf4j
public class NameNodeClient {

    private final DefaultScheduler defaultScheduler;
    private final BackupNodeConfig backupnodeConfig;
    private final NetClient netClient;
    private final InMemoryNameSystem nameSystem;
    private volatile boolean shutdown = false;

    public NameNodeClient(DefaultScheduler defaultScheduler, BackupNodeConfig backupnodeConfig, InMemoryNameSystem nameSystem) {
        this.netClient = new NetClient("BackupNode-NameNode-" + backupnodeConfig.getNameNodeHostname(), defaultScheduler, 3);
        this.defaultScheduler = defaultScheduler;
        this.nameSystem = nameSystem;
        this.backupnodeConfig = backupnodeConfig;
    }

    /**
     * 和NameNode建立链接
     */
    public void start() {
        this.netClient.addNetClientFailListener(() -> {
            shutdown = true;
            log.info("BackupNode检测到NameNode挂了，进行升级为NameNode的步骤...");
            try {
                NodeRoleSwitcher.getInstance().maybeUpgradeToNameNode();
            } catch (Exception e) {
                log.error("NodeRoleSwitcher#maybeUpgradeToNameNode occurs error.", e);
            }
        });
        this.netClient.addConnectListener(connected -> {
            if (connected) {
                reportBackupNodeInfo();
            }
        });
        this.netClient.addNettyPackageListener(requestWrapper -> {
            NettyPacket request = requestWrapper.getRequest();
            if (request.getPacketType() == PacketType.BACKUP_NODE_SLOT.getValue()) {
                log.info("收到NameNode下发的Slots信息.");
                BackupNodeSlots backupNodeSlots = BackupNodeSlots.parseFrom(request.getBody());
                NodeRoleSwitcher.getInstance().setSlots(backupNodeSlots.getSlotsMap());
            }
        });
        this.netClient.connect(backupnodeConfig.getNameNodeHostname(), backupnodeConfig.getNameNodePort());
        EditsLogFetcher editsLogFetcher = new EditsLogFetcher(backupnodeConfig, this, nameSystem);
        defaultScheduler.schedule("抓取editLog", editsLogFetcher,
                backupnodeConfig.getFetchEditLogInterval(), backupnodeConfig.getFetchEditLogInterval(), TimeUnit.MILLISECONDS);
        FsImageCheckPointer fsImageCheckpointer = new FsImageCheckPointer(this, nameSystem, backupnodeConfig);
        defaultScheduler.schedule("FSImage Checkpoint操作", fsImageCheckpointer,
                backupnodeConfig.getCheckpointInterval(), backupnodeConfig.getCheckpointInterval(), TimeUnit.MILLISECONDS);
    }

    /**
     * 主动往NameNode上报自己的信息
     */
    private void reportBackupNodeInfo() {
        // 因为这里有阻塞方法，回调中不能阻塞，
        // 否则会导致后面网络收发失败，所以需要新开线程处理
        defaultScheduler.scheduleOnce("往NameNode上报自己的信息", () -> {
            try {
                BackupNodeInfo backupNodeInfo = BackupNodeInfo.newBuilder()
                        .setHostname(backupnodeConfig.getBackupNodeHostname())
                        .setPort(backupnodeConfig.getBackupNodePort())
                        .build();
                NettyPacket req = NettyPacket.buildPacket(backupNodeInfo.toByteArray(), PacketType.REPORT_BACKUP_NODE_INFO);
                log.info("上报BackupNode连接信息：[hostname={}, port={}]", backupnodeConfig.getBackupNodeHostname(),
                        backupnodeConfig.getBackupNodePort());
                NettyPacket nettyPacket = netClient.sendSync(req);
                if (nettyPacket.getPacketType() == PacketType.DUPLICATE_BACKUP_NODE.getValue()) {
                    log.error("该NameNode已经存在一个BackupNode了，出现重复的BackupNode, 程序即将退出...");
                    System.exit(0);
                    return;
                }
                NameNodeConf nameNodeConf = NameNodeConf.parseFrom(nettyPacket.getBody());
                log.info("上报BackupNode连接信息，返回的NameNode配置信息: [values={}]", nameNodeConf.getValuesMap());
                NodeRoleSwitcher.getInstance().setNameNodeConfig(new NameNodeConfig(nameNodeConf));
            } catch (Exception e) {
                log.error("上报BackupNode信息发生错误：", e);
            }
        });
    }

    /**
     * 抓取editLog数据
     *
     * @param txId 当前的txId
     * @return editLog数据
     */
    public List<EditLog> fetchEditsLog(long txId) throws InvalidProtocolBufferException, InterruptedException, RequestTimeoutException {
        boolean hasSlots = NodeRoleSwitcher.getInstance().hasSlots();
        FetchEditsLogRequest request = FetchEditsLogRequest.newBuilder()
                .setTxId(txId)
                .setNeedSlots(!hasSlots)
                .build();
        NettyPacket req = NettyPacket.buildPacket(request.toByteArray(), PacketType.FETCH_EDIT_LOG);
        NettyPacket nettyPacket = netClient.sendSync(req);
        FetchEditsLogResponse response = FetchEditsLogResponse.parseFrom(nettyPacket.getBody());
        List<UserEntity> usersList = response.getUsersList();
        NodeRoleSwitcher.getInstance().replaceUser(usersList.stream()
                .map(User::parse)
                .collect(Collectors.toList()));
        return response.getEditLogsList();
    }

    public NetClient getNetClient() {
        return netClient;
    }

    public DefaultScheduler getDefaultScheduler() {
        return defaultScheduler;
    }

    /**
     * 优雅停机
     */
    public void shutdown() {
        this.netClient.shutdown();
    }
}
