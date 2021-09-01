package com.ruyuan.dfs.namenode.shard.controller;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ruyuan.dfs.common.FileInfo;
import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.enums.NodeType;
import com.ruyuan.dfs.common.enums.PacketType;
import com.ruyuan.dfs.common.network.RequestWrapper;
import com.ruyuan.dfs.model.namenode.*;
import com.ruyuan.dfs.namenode.datanode.DataNodeManager;
import com.ruyuan.dfs.namenode.fs.DiskNameSystem;
import com.ruyuan.dfs.namenode.shard.peer.PeerNameNodes;
import com.ruyuan.dfs.namenode.shard.rebalance.RebalanceSlotInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * 基于远程PeerNode的Controller
 *
 * @author Sun Dasheng
 */
@Slf4j
public class RemoteController extends AbstractController {

    public RemoteController(int nameNodeId, int controllerId, PeerNameNodes peerNameNodes, DiskNameSystem diskNameSystem,
                            DataNodeManager dataNodeManager) {
        super(nameNodeId, controllerId, peerNameNodes, diskNameSystem, dataNodeManager);
    }

    @Override
    public Map<Integer, Integer> initSlotAllocate() throws Exception {
        while (!initCompleted.get()) {
            lock.lock();
            try {
                initCompletedCondition.await();
            } finally {
                lock.unlock();
            }
        }
        Set<Integer> currentSlots = nodeSlotsMap.get(nameNodeId);
        log.info("初始化槽位信息：[nodeId={}, slots size={}]", nameNodeId, currentSlots.size());
        return Collections.unmodifiableMap(slotNodeMap);
    }

    @Override
    protected void replaceSlots(Map<Integer, Integer> slotNodeMap, Map<Integer, Set<Integer>> nodeSlotsMap) throws Exception {
        super.replaceSlots(slotNodeMap, nodeSlotsMap);
        lock.lock();
        this.initCompleted.set(true);
        try {
            initCompletedCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onReceiveSlots(NameNodeSlots nameNodeSlots) throws Exception {
        synchronized (this) {
            Map<Integer, Integer> oldSlotsMap = nameNodeSlots.getOldSlotsMap();
            Map<Integer, Integer> newSlotsMap = nameNodeSlots.getNewSlotsMap();
            if (nameNodeSlots.getRebalance()) {
                maybeFetchMetadata(oldSlotsMap, newSlotsMap, nameNodeSlots.getRebalanceNodeId());
            } else {
                Map<Integer, Integer> replaceSlotsNodeMap = new HashMap<>(newSlotsMap);
                Map<Integer, Set<Integer>> replaceNodeSlotsMap = map(replaceSlotsNodeMap);
                replaceSlots(replaceSlotsNodeMap, replaceNodeSlotsMap);
            }
        }
    }

    /**
     * 如果有必要，则从别的NameNode中同步元数据
     *
     * @param oldSlotsMap     旧的Slots分配信息
     * @param newSlotsMap     新的Slots分配信息
     * @param rebalanceNodeId 需要重平衡的ID
     * @throws Exception 中断异常
     */
    private void maybeFetchMetadata(Map<Integer, Integer> oldSlotsMap, Map<Integer, Integer> newSlotsMap,
                                    int rebalanceNodeId) throws Exception {
        /*
         * 对于新上线节点来说：
         *
         *    oldSlot = []
         *    newSlot = [1, 2, 3]
         *
         * 则需要将[1, 2, 3]从别的节点中获取过来
         *
         * 对于旧节点来说：
         *
         *    oldSlot = [1, 2, 3, 4, 5, 6]
         *    newSlot = [4, 5, 6]
         *
         * 则不需要做任何事情
         *
         */
        log.info("收到重平衡后的Slots信息广播，当次重平衡发起人为：[rebalanceNodeId={}]", rebalanceNodeId);
        rebalanceSlotInfo = new RebalanceSlotInfo();
        rebalanceSlotInfo.setApplyRebalanceNodeId(rebalanceNodeId);
        rebalanceSlotInfo.setSlotNodeMapSnapshot(newSlotsMap);
        Set<Integer> currentSlots = rebalanceSlotInfo.getSlotsFor(nameNodeId);
        Map<Integer, Set<Integer>> otherNodeSlots = new HashMap<>(peerNameNodes.getAllServers().size());
        for (Integer slotIndex : currentSlots) {
            Integer oldSlotNameNodeId = oldSlotsMap.get(slotIndex);
            if (oldSlotNameNodeId != nameNodeId) {
                // 这部分槽的元数据在别的节点
                Set<Integer> slots = otherNodeSlots.computeIfAbsent(oldSlotNameNodeId, k -> new HashSet<>());
                slots.add(slotIndex);
            }
        }
        if (otherNodeSlots.isEmpty()) {
            log.info("本次重平衡，我不需要从别的节点获取元数据：[rebalanceNodeId={}]", rebalanceNodeId);
            return;
        }
        log.info("本次重平衡，我需要从别的节点获取元数据：[targetNodeId={}]", otherNodeSlots.keySet());
        for (Map.Entry<Integer, Set<Integer>> entry : otherNodeSlots.entrySet()) {
            Integer targetNodeId = entry.getKey();
            FetchMetaDataRequest fetchDataBySlotRequest = FetchMetaDataRequest.newBuilder()
                    .addAllSlots(entry.getValue())
                    .setNodeId(nameNodeId)
                    .build();
            NettyPacket nettyPacket = NettyPacket.buildPacket(fetchDataBySlotRequest.toByteArray(), PacketType.FETCH_SLOT_METADATA);
            peerNameNodes.send(targetNodeId, nettyPacket);
            // 这里发送后会走到 ControllerManger#writeMetadataToPeer方法，接着会走到自身节点的onFetchMetadata方法
            log.info("发送请求从别的NameNode获取元数据：[targetNodeId={}, slotsSize={}]", targetNodeId, entry.getValue().size());
        }
        rebalanceSlotInfo.waitFetchMetadataCompleted(otherNodeSlots.keySet());
        replaceSlots(rebalanceSlotInfo.getSlotNodeMapSnapshot(), rebalanceSlotInfo.getNodeSlotsMapSnapshot());
        // 发送个请求给Controller, 告诉它我已经完事了，可以对外工作了。
        RebalanceFetchMetadataCompletedEvent event = RebalanceFetchMetadataCompletedEvent.newBuilder()
                .setRebalanceNodeId(nameNodeId)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(event.toByteArray(), PacketType.FETCH_SLOT_METADATA_COMPLETED);
        peerNameNodes.send(controllerId, nettyPacket);
        log.info("恭喜，所有节点的元数据都拉取回来了，自己可以对外工作了，同时发送请求给Controller表示自己已经完成了: [targetNodeId={}]",
                otherNodeSlots.keySet());
    }

    @Override
    public void rebalanceSlots(NettyPacket nettyPacket) throws Exception {
        if (initCompleted.get()) {
            log.info("从磁盘中读取到Slots信息，不需要重新分配了. [nodeId={}]", nameNodeId);
        } else {
            peerNameNodes.send(controllerId, nettyPacket);
            log.info("发送请求到Controller节点申请重平衡. [controllerId={}]", controllerId);
        }
    }

    @Override
    public void onFetchMetadata(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        FetchMetaDataResponse response = FetchMetaDataResponse.parseFrom(requestWrapper.getRequest().getBody());
        log.info("收到PeerNameNode的元数据信息：[peerNodeId={}, isCompleted={}, fileSize={}]",
                response.getNodeId(), response.getCompleted(), response.getFilesCount());
        if (rebalanceSlotInfo.getApplyRebalanceNodeId() != this.nameNodeId) {
            log.warn("提示：本轮重平衡不是自己发起的，别人发起的重平衡把我的Slot信息也分配好了。");
        }
        List<Metadata> filesList = response.getFilesList();
        for (Metadata metadata : filesList) {
            if (NodeType.FILE.getValue() == metadata.getType()) {
                diskNameSystem.createFile(metadata.getFileName(), metadata.getAttrMap());
                FileInfo fileInfo = new FileInfo();
                fileInfo.setHostname(metadata.getHostname());
                fileInfo.setFileSize(metadata.getFileSize());
                fileInfo.setFileName(metadata.getFileName());
                dataNodeManager.addReplica(fileInfo);
            } else if (NodeType.DIRECTORY.getValue() == metadata.getType()) {
                diskNameSystem.mkdir(metadata.getFileName(), metadata.getAttrMap());
            }
        }
        if (response.getCompleted()) {
            // 标记其中一个节点的元数据已经全部收到
            rebalanceSlotInfo.onNodeCompletedFetchMetadata(response.getNodeId());
        }
    }

    @Override
    public void onFetchSlotMetadataCompleted(int rebalanceNodeId) throws Exception {
        removeMetadata(rebalanceNodeId);
        RebalanceRemoveMetadataCompletedEvent event = RebalanceRemoveMetadataCompletedEvent.newBuilder()
                .setCurrentNameNodeId(this.nameNodeId)
                .setRebalanceNodeId(rebalanceNodeId)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(event.toByteArray(), PacketType.REMOVE_METADATA_COMPLETED);
        this.peerNameNodes.send(controllerId, nettyPacket);
        this.rebalanceSlotInfo = null;
        log.info("已经移除了内存中不属于自己Slots的元数据，发送通知告诉Controller: [controllerNodeId={}]", controllerId);
    }
}
