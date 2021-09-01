package com.ruyuan.dfs.namenode.shard.rebalance;

import lombok.Data;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 重平衡Slots信息
 *
 * @author Sun Dasheng
 */
@Data
public class RebalanceSlotInfo {

    /**
     * 申请发起重平衡的节点ID
     */
    private int applyRebalanceNodeId;

    /**
     * 所有节点的ID
     */
    private List<Integer> nodeIdList;

    /**
     * 本次重平衡包含的节点ID列表
     */
    private Set<Integer> rebalanceNodeIdSet;

    /**
     * Slots分配的快照
     */
    private Map<Integer, Integer> slotNodeMapSnapshot;

    /**
     * Slots分配的快照
     */
    private Map<Integer, Set<Integer>> nodeSlotsMapSnapshot;
    private ReentrantLock lock;

    private Set<Integer> fetchMetadataWaitNodeSet;
    private Set<Integer> fetchMetadataCompletedNodeSet;
    private Condition fetchMetadataCompletedCondition;

    private CountDownLatch removeMetadataCompleteCountDownLatch;

    private Set<Integer> removeMetadataCompletedNodeSet = new HashSet<>();

    public RebalanceSlotInfo() {
        this.lock = new ReentrantLock();
        this.fetchMetadataCompletedCondition = lock.newCondition();
    }


    /**
     * 设置快照信息
     *
     * @param slotNodeMapSnapshot 快照
     */
    public void setSlotNodeMapSnapshot(Map<Integer, Integer> slotNodeMapSnapshot) {
        this.slotNodeMapSnapshot = slotNodeMapSnapshot;
        this.nodeSlotsMapSnapshot = new HashMap<>(2);
        for (Map.Entry<Integer, Integer> entry : slotNodeMapSnapshot.entrySet()) {
            Set<Integer> slots = nodeSlotsMapSnapshot.computeIfAbsent(entry.getValue(), k -> new HashSet<>());
            slots.add(entry.getKey());
        }
    }


    public Set<Integer> getSlotsFor(int nodeId) {
        return nodeSlotsMapSnapshot.getOrDefault(nodeId, new HashSet<>());
    }


    /**
     * 等到整个重平衡过程结束
     * <pre>
     *     比如一开始3个节点[1, 2, 3]
     *     此时新加入2个节点[1, 2, 3, 4, 5]
     *
     *    那么需要等待[4, 5]两个节点都完成了拉取元数据，
     *    并且等到 [1, 2] 上报移除了不属于自己的元数据的时候，这里才算重平衡结束 (3号节点是Controller，不用等待)
     *
     *
     * </pre>
     *
     * @param rebalanceNodeIdList 本次重平衡的需要的节点数量
     * @throws InterruptedException 异常
     */
    public void waitRemoveMetadataCompleted(Set<Integer> rebalanceNodeIdList) throws InterruptedException {
        this.removeMetadataCompleteCountDownLatch = new CountDownLatch(nodeIdList.size() - rebalanceNodeIdList.size() - 1);
        this.removeMetadataCompleteCountDownLatch.await();
    }


    public boolean onFetchSlotMetadataCompleted(int rebalanceNodeId) {
        synchronized (this) {
            removeMetadataCompletedNodeSet.add(rebalanceNodeId);
            return isAllCompleted(rebalanceNodeIdSet, removeMetadataCompletedNodeSet);
        }
    }

    /**
     * 某个节点完成所有的元数据删除
     */
    public boolean onNodeCompletedRemoveMetadata() {
        removeMetadataCompleteCountDownLatch.countDown();
        return removeMetadataCompleteCountDownLatch.getCount() == 0;
    }


    private boolean isAllCompleted(Set<Integer> waitSet, Set<Integer> completedSet) {
        boolean allCompleted = true;
        for (Integer nodeId : waitSet) {
            if (!completedSet.contains(nodeId)) {
                allCompleted = false;
                break;
            }
        }
        return allCompleted;
    }


    /**
     * 作为新加入的节点等待其他NameNode把所有的元数据发送过来
     *
     * @param waitMetadataNodeSet 目标列表
     * @throws InterruptedException 异常
     */
    public void waitFetchMetadataCompleted(Set<Integer> waitMetadataNodeSet) throws InterruptedException {
        this.fetchMetadataCompletedNodeSet = new HashSet<>();
        this.fetchMetadataWaitNodeSet = waitMetadataNodeSet;
        ReentrantLock lock = this.lock;
        lock.lock();
        try {
            fetchMetadataCompletedCondition.await();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 某个节点完成所有的元数据同步
     *
     * @param nameNodeId 节点ID
     */
    public void onNodeCompletedFetchMetadata(int nameNodeId) {
        synchronized (this) {
            fetchMetadataCompletedNodeSet.add(nameNodeId);
            boolean allCompleted = isAllCompleted(fetchMetadataWaitNodeSet, fetchMetadataCompletedNodeSet);
            if (allCompleted) {
                lock.lock();
                try {
                    fetchMetadataCompletedCondition.signal();
                } finally {
                    lock.unlock();
                }
            }
        }
    }
}
