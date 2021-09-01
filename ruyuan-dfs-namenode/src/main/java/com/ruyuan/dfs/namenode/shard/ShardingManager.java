package com.ruyuan.dfs.namenode.shard;

import com.ruyuan.dfs.common.Constants;
import com.ruyuan.dfs.common.enums.NameNodeLaunchMode;
import com.ruyuan.dfs.common.utils.StringUtils;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.shard.controller.ControllerManager;
import com.ruyuan.dfs.namenode.shard.controller.OnSlotAllocateCompletedListener;
import com.ruyuan.dfs.namenode.shard.peer.PeerNameNodes;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * 负责元数据分片的组件
 *
 * @author Sun Dasheng
 */
@Slf4j
public class ShardingManager {

    private PeerNameNodes peerNameNodes;
    private NameNodeConfig nameNodeConfig;
    private ControllerManager controllerManager;

    public ShardingManager(NameNodeConfig nameNodeConfig, PeerNameNodes peerNameNodes, ControllerManager controllerManager) {
        this.nameNodeConfig = nameNodeConfig;
        this.peerNameNodes = peerNameNodes;
        this.controllerManager = controllerManager;
    }

    /**
     * 启动
     */
    public void start() throws Exception {
        log.info("NameNode模式模式：[mode={}]", nameNodeConfig.getMode());
        if (NameNodeLaunchMode.SINGLE.equals(nameNodeConfig.getMode())) {
            return;
        }
        log.info("NameNode当前节点为：[nodeId={}, baseDir={}]", nameNodeConfig.getNameNodeId(), nameNodeConfig.getBaseDir());
        if (nameNodeConfig.getNameNodePeerServers() == null || nameNodeConfig.getNameNodePeerServers().length() == 0
                || nameNodeConfig.getNameNodePeerServers().split(",").length == 1) {
            log.info("NameNode集群模式为单点模式, 自己就是Controller节点");
            controllerManager.startControllerElection();
        } else {
            String nameNodePeerServers = nameNodeConfig.getNameNodePeerServers();
            String[] nodeServer = nameNodePeerServers.split(",");
            for (String server : nodeServer) {
                peerNameNodes.connect(server);
            }
        }
    }

    /**
     * 根据文件名获取该文件属于哪个slot，返回该slot所在的nameNodeId
     *
     * @param filename 文件名
     * @return 节点ID
     */
    public int getNameNodeIdByFileName(String filename) {
        int slot = StringUtils.hash(filename, Constants.SLOTS_COUNT);
        return controllerManager.getNodeIdBySlot(slot);
    }

    /**
     * 添加Slot分配信息完成后的监听器
     *
     * @param listener 监听器
     */
    public void addOnSlotAllocateCompletedListener(OnSlotAllocateCompletedListener listener) {
        controllerManager.addOnSlotAllocateCompletedListener(listener);
    }

    public Map<Integer, Integer> getSlotNodeMap() {
        return controllerManager.getSlotNodeMap();
    }
}
