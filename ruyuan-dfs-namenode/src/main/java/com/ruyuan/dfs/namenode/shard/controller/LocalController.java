package com.ruyuan.dfs.namenode.shard.controller;

import com.ruyuan.dfs.common.Constants;
import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.enums.PacketType;
import com.ruyuan.dfs.model.datanode.RegisterRequest;
import com.ruyuan.dfs.model.namenode.*;
import com.ruyuan.dfs.namenode.datanode.DataNodeInfo;
import com.ruyuan.dfs.namenode.datanode.DataNodeManager;
import com.ruyuan.dfs.namenode.fs.DiskNameSystem;
import com.ruyuan.dfs.namenode.server.UserManager;
import com.ruyuan.dfs.namenode.server.tomcat.domain.User;
import com.ruyuan.dfs.namenode.shard.peer.PeerNameNodes;
import com.ruyuan.dfs.namenode.shard.rebalance.RebalanceManager;
import com.ruyuan.dfs.namenode.shard.rebalance.RebalanceSlotInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 本地 Controller
 *
 * @author Sun Dasheng
 */
@Slf4j
public class LocalController extends AbstractController {

    private UserManager userManager;
    private RebalanceManager rebalanceManager;

    public LocalController(int nameNodeId, PeerNameNodes peerNameNodes, DiskNameSystem diskNameSystem,
                           DataNodeManager dataNodeManager, UserManager userManager) {
        super(nameNodeId, nameNodeId, peerNameNodes, diskNameSystem, dataNodeManager);
        this.rebalanceManager = new RebalanceManager(this);
        this.userManager = userManager;
    }

    @Override
    public Map<Integer, Integer> initSlotAllocate() throws Exception {
        if (initCompleted.get()) {
            log.info("从磁盘中读取到Slots信息，作为Controller不需要重新分配Slots了. [nodeId={}]", nameNodeId);
        } else {
            List<Integer> allNodeId = peerNameNodes.getAllNodeId();
            allNodeId.add(nameNodeId);
            Map<Integer, Integer> slotNodeMap = new HashMap<>(Constants.SLOTS_COUNT);
            Map<Integer, Set<Integer>> nodeSlotsMap = new HashMap<>(peerNameNodes.getAllServers().size());
            for (int i = 0; i < Constants.SLOTS_COUNT; i++) {
                int index = i % allNodeId.size();
                Integer nodeId = allNodeId.get(index);
                slotNodeMap.put(i, nodeId);
                Set<Integer> slots = nodeSlotsMap.computeIfAbsent(nodeId, k -> new HashSet<>());
                slots.add(i);
            }
            replaceSlots(slotNodeMap, nodeSlotsMap);
            log.info("作为Controller分配好槽位信息, 发送广播给所有的PeerNameNode：[nodeId={}]", nameNodeId);
            NameNodeSlots nameNodeSlots = NameNodeSlots.newBuilder()
                    .putAllOldSlots(slotNodeMap)
                    .putAllNewSlots(slotNodeMap)
                    .setRebalance(false)
                    .build();
            NettyPacket nettyPacket = NettyPacket.buildPacket(nameNodeSlots.toByteArray(), PacketType.NAME_NODE_SLOT_BROADCAST);
            peerNameNodes.broadcast(nettyPacket);
        }
        Set<Integer> currentSlots = nodeSlotsMap.get(nameNodeId);
        log.info("初始化槽位信息：[nodeId={}, slots size={}]", nameNodeId, currentSlots.size());
        return Collections.unmodifiableMap(slotNodeMap);
    }

    /**
     * <pre>
     *
     * @param nettyPacket 网络包
     */
    @Override
    public void rebalanceSlots(NettyPacket nettyPacket) throws Exception {
        synchronized (this) {
            List<Integer> allNodeId = peerNameNodes.getAllNodeId();
            allNodeId.add(nameNodeId);
            int numOfNode = allNodeId.size();
            int oldNumOfNode = nodeSlotsMap.size();
            if (numOfNode <= oldNumOfNode) {
                log.warn("节点数量异常，不需要重新分配slots");
                return;
            }
            RebalanceSlotsRequest rebalanceSlotsRequest = RebalanceSlotsRequest.parseFrom(nettyPacket.getBody());
            RebalanceSlotInfo info = new RebalanceSlotInfo();
            info.setApplyRebalanceNodeId(rebalanceSlotsRequest.getNameNodeId());
            info.setNodeIdList(allNodeId);
            // 这里使用防御性复制，避免别的线程改动了这两个map
            HashMap<Integer, Integer> slotNodeMapSnapshot = new HashMap<>(slotNodeMap);
            info.setSlotNodeMapSnapshot(slotNodeMapSnapshot);
            rebalanceManager.add(info);
        }
    }

    /**
     * 重平衡结果下发
     *
     * @param rebalanceSlotInfo 重平衡结果
     */
    public void setRebalanceInfo(RebalanceSlotInfo rebalanceSlotInfo) throws InterruptedException {
        synchronized (this) {
            this.rebalanceSlotInfo = rebalanceSlotInfo;
            NameNodeSlots nameNodeSlots = NameNodeSlots.newBuilder()
                    .putAllOldSlots(slotNodeMap)
                    .putAllNewSlots(rebalanceSlotInfo.getSlotNodeMapSnapshot())
                    .setRebalance(true)
                    .setRebalanceNodeId(rebalanceSlotInfo.getApplyRebalanceNodeId())
                    .build();
            NettyPacket nettyPacket = NettyPacket.buildPacket(nameNodeSlots.toByteArray(), PacketType.NAME_NODE_SLOT_BROADCAST);
            peerNameNodes.broadcast(nettyPacket);
            log.info("重平衡Slot之后，将最新的Slots分配信息广播给所有的NameNode.");

            Set<Integer> rebalanceNodeIdSet = rebalanceSlotInfo.getRebalanceNodeIdSet();
            List<DataNodeInfo> dataNodeInfoList = dataNodeManager.getDataNodeInfoList();
            NewPeerNodeInfo.Builder builder = NewPeerNodeInfo.newBuilder();
            for (DataNodeInfo dataNodeInfo : dataNodeInfoList) {
                RegisterRequest request = RegisterRequest.newBuilder()
                        .setHostname(dataNodeInfo.getHostname())
                        .setHttpPort(dataNodeInfo.getHttpPort())
                        .setNioPort(dataNodeInfo.getNioPort())
                        .setStoredDataSize(dataNodeInfo.getStoredDataSize())
                        .setFreeSpace(dataNodeInfo.getFreeSpace())
                        .setNodeId(dataNodeInfo.getNodeId())
                        .build();
                builder.addRequests(request);
            }
            List<UserEntity> userEntities = userManager.getAllUser().stream()
                    .map(User::toEntity)
                    .collect(Collectors.toList());
            builder.addAllUsers(userEntities);
            NewPeerNodeInfo newPeerDataNodeInfo = builder.build();
            NettyPacket request = NettyPacket.buildPacket(newPeerDataNodeInfo.toByteArray(), PacketType.NEW_PEER_NODE_INFO);
            for (Integer nodeId : rebalanceNodeIdSet) {
                peerNameNodes.send(nodeId, request);
            }
            log.info("下发所有DataNode信息给本次重平衡包含的节点：[nodeIds={}]", rebalanceNodeIdSet);
        }
    }

    @Override
    public void onFetchSlotMetadataCompleted(int rebalanceNodeId) {
        // rebalanceNodeId表示本轮重平衡可能的id
        boolean isAllCompleted = rebalanceSlotInfo.onFetchSlotMetadataCompleted(rebalanceNodeId);
        if (isAllCompleted) {
            RebalanceFetchMetadataCompletedEvent event = RebalanceFetchMetadataCompletedEvent.newBuilder()
                    .setRebalanceNodeId(rebalanceSlotInfo.getApplyRebalanceNodeId())
                    .build();
            NettyPacket nettyPacket = NettyPacket.buildPacket(event.toByteArray(), PacketType.FETCH_SLOT_METADATA_COMPLETED_BROADCAST);
            List<Integer> otherNodeIds = peerNameNodes.broadcast(nettyPacket, rebalanceSlotInfo.getRebalanceNodeIdSet());
            log.info("发送广播给所有的NameNode节点，让他们移除所有的元数据: [applyRebalanceNodeId={}, rebalanceNodeId={}, otherNodeIds={}]",
                    rebalanceSlotInfo.getApplyRebalanceNodeId(), rebalanceNodeId, otherNodeIds == null ? 0 : otherNodeIds.size());
        }
    }

    @Override
    public void onRemoveMetadataCompleted(NettyPacket nettyPacket) throws Exception {
        synchronized (this) {
            RebalanceRemoveMetadataCompletedEvent event = RebalanceRemoveMetadataCompletedEvent.parseFrom(nettyPacket.getBody());
            int currentNameNodeId = event.getCurrentNameNodeId();
            int rebalanceNodeId = event.getRebalanceNodeId();
            log.info("Controller收到NameNode节点完成元数据删除的信息：[nameNodeId={}, rebalanceNodeId={}]", currentNameNodeId, rebalanceNodeId);
            boolean isAllCompleted = this.rebalanceSlotInfo.onNodeCompletedRemoveMetadata();
            if (isAllCompleted) {
                removeMetadata(rebalanceSlotInfo.getApplyRebalanceNodeId());
                this.rebalanceSlotInfo = null;
            }
        }
    }

}
