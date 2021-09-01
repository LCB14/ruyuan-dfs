package com.ruyuan.dfs.namenode.shard.controller;

import com.google.common.collect.Sets;
import com.ruyuan.dfs.common.utils.FileUtil;
import com.ruyuan.dfs.model.namenode.Metadata;
import com.ruyuan.dfs.model.namenode.NameNodeSlots;
import com.ruyuan.dfs.namenode.datanode.DataNodeManager;
import com.ruyuan.dfs.namenode.fs.DiskNameSystem;
import com.ruyuan.dfs.namenode.shard.peer.PeerNameNodes;
import com.ruyuan.dfs.namenode.shard.rebalance.RebalanceSlotInfo;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 抽象controller
 *
 * @author Sun Dasheng
 */
@Slf4j
public abstract class AbstractController implements Controller {
    protected DataNodeManager dataNodeManager;
    protected DiskNameSystem diskNameSystem;
    protected int controllerId;
    protected int nameNodeId;
    protected PeerNameNodes peerNameNodes;
    protected Map<Integer, Integer> slotNodeMap;
    protected Map<Integer, Set<Integer>> nodeSlotsMap;
    protected AtomicBoolean initCompleted;
    protected RebalanceSlotInfo rebalanceSlotInfo;
    protected ReentrantLock lock;
    protected Condition initCompletedCondition;
    private final List<OnSlotAllocateCompletedListener> slotAllocateCompletedListeners = new ArrayList<>();

    public AbstractController(int nameNodeId, int controllerId, PeerNameNodes peerNameNodes, DiskNameSystem diskNameSystem,
                              DataNodeManager dataNodeManager) {
        this.nameNodeId = nameNodeId;
        this.peerNameNodes = peerNameNodes;
        this.controllerId = controllerId;
        this.diskNameSystem = diskNameSystem;
        this.dataNodeManager = dataNodeManager;
        this.initCompleted = new AtomicBoolean(false);
        this.lock = new ReentrantLock();
        this.initCompletedCondition = lock.newCondition();
        readDiskSlots();
    }

    /**
     * 应用新的slot分配信息
     *
     * @param slotNodeMap  slot信息
     * @param nodeSlotsMap slot信息
     */
    protected void replaceSlots(Map<Integer, Integer> slotNodeMap, Map<Integer, Set<Integer>> nodeSlotsMap) throws Exception {
        this.slotNodeMap = slotNodeMap;
        this.nodeSlotsMap = nodeSlotsMap;
        // 持久化到磁盘
        String slotFile = diskNameSystem.getNameNodeConfig().getSlotFile();
        NameNodeSlots slots = NameNodeSlots.newBuilder()
                .putAllNewSlots(this.slotNodeMap)
                .build();
        ByteBuffer buffer = ByteBuffer.wrap(slots.toByteArray());
        FileUtil.saveFile(slotFile, true, buffer);
        invokeSlotAllocateCompleted();
        log.info("保存槽位信息到磁盘中：[nodeId={}]", nameNodeId);
    }

    protected void readDiskSlots() {
        try {
            String slotFile = diskNameSystem.getNameNodeConfig().getSlotFile();
            File file = new File(slotFile);
            if (!file.exists()) {
                return;
            }
            try (RandomAccessFile raf = new RandomAccessFile(slotFile, "r"); FileInputStream fis =
                    new FileInputStream(raf.getFD()); FileChannel channel = fis.getChannel()) {
                ByteBuffer buffer = ByteBuffer.allocate((int) raf.length());
                channel.read(buffer);
                buffer.flip();
                NameNodeSlots nameNodeSlots = NameNodeSlots.parseFrom(buffer);
                Map<Integer, Integer> slotNodeMap = nameNodeSlots.getNewSlotsMap();
                Map<Integer, Set<Integer>> nodeSlotsMap = map(slotNodeMap);
                this.slotNodeMap = slotNodeMap;
                this.nodeSlotsMap = nodeSlotsMap;
                this.initCompleted.set(true);
                invokeSlotAllocateCompleted();
                log.info("从磁盘中恢复槽位信息：[nodeId={}]", nameNodeId);
            }
        } catch (Exception e) {
            log.info("恢复磁盘中的Slots文件失败：", e);
        }
    }

    protected Map<Integer, Set<Integer>> map(Map<Integer, Integer> slotNodeMap) {
        Map<Integer, Set<Integer>> nodeSlotsMap = new HashMap<>(2);
        for (Map.Entry<Integer, Integer> entry : slotNodeMap.entrySet()) {
            Set<Integer> slots = nodeSlotsMap.computeIfAbsent(entry.getValue(), k -> new HashSet<>());
            slots.add(entry.getKey());
        }
        return nodeSlotsMap;
    }

    /**
     * 执行元数据删除逻辑
     *
     * @param rebalanceNodeId 重平衡节点ID
     */
    public void removeMetadata(int rebalanceNodeId) throws Exception {
        log.info("开始执行内存元数据删除. [rebalanceNodeId={}]", rebalanceNodeId);
        Map<Integer, Set<Integer>> oldSlotNodeMap = nodeSlotsMap;
        replaceSlots(rebalanceSlotInfo.getSlotNodeMapSnapshot(), rebalanceSlotInfo.getNodeSlotsMapSnapshot());
        Map<Integer, Set<Integer>> newSlotNodeMap = nodeSlotsMap;

        Set<Integer> oldCurrentSlots = oldSlotNodeMap.get(this.nameNodeId);
        Set<Integer> newCurrentSlots = newSlotNodeMap.get(this.nameNodeId);
        Set<Integer> toRemoveMetadataSlots = Sets.difference(oldCurrentSlots, newCurrentSlots);

        // 移除不属于自己的元数据，释放内存空间
        for (Integer toRemoveSlot : toRemoveMetadataSlots) {
            Set<Metadata> metadataSet = diskNameSystem.getFilesBySlot(toRemoveSlot);
            if (metadataSet == null) {
                continue;
            }
            for (Metadata metadata : metadataSet) {
                dataNodeManager.removeFileStorage(metadata.getFileName(), false);
            }
        }
    }

    private void invokeSlotAllocateCompleted() {
        Map<Integer, Integer> slotsMap = Collections.unmodifiableMap(slotNodeMap);
        for (OnSlotAllocateCompletedListener listener : slotAllocateCompletedListeners) {
            listener.onSlotAllocateCompleted(slotsMap);
        }
    }

    @Override
    public Map<Integer, Integer> getSlotNodeMap() {
        return Collections.unmodifiableMap(slotNodeMap);
    }

    @Override
    public void addOnSlotAllocateCompletedListener(OnSlotAllocateCompletedListener listener) {
        slotAllocateCompletedListeners.add(listener);
    }

    @Override
    public int getNodeIdBySlot(int slot) {
        return slotNodeMap.get(slot);
    }
}
