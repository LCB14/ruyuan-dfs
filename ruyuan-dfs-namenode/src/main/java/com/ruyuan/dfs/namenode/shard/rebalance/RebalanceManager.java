package com.ruyuan.dfs.namenode.shard.rebalance;

import com.ruyuan.dfs.common.Constants;
import com.ruyuan.dfs.common.utils.NamedThreadFactory;
import com.ruyuan.dfs.namenode.shard.controller.LocalController;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 负责重平衡的组件
 *
 * <pre>
 *   1 判断需要重平衡的节点是否在上一轮已经考虑进去了，给它分配过槽位了？如果是，跳过此请求，如果否，则Controller进行重新分配slot
 *
 *   2 Controller重新分配好Slot广播新的slot分配信息，暂存起来
 *
 *   3 所有的NameNode节点收到广播的slot分配信息，识别出是重平衡的信息，先将新的slot信息暂存起来(不替换,还未生效)
 *
 *   4 新加入的NameNode节点，从广播的Slot分配信息中识别出：我有哪些slot，每个slot对应的元数据应该取哪个NameNode节点获取
 *
 *   5 新加入的NameNode节点，去每个NameNode节点拉取元数据，确保所有slot的元数据都拉取回来之后，上报给Controller，表示自己已经可以对外工作了
 *
 *   6 Controller替换让暂存的slot分配信息生效，同时广播给所有的NameNode节点，让他们把本轮重平衡需要删除的元数据删除，并让自己暂存的slot分配信息生效
 *
 *   7 此时完成一轮重平衡。才可以进行下一轮重平衡
 *
 * </pre>
 *
 * @author Sun Dasheng
 */
@Slf4j
public class RebalanceManager {

    private LocalController localController;
    private LinkedBlockingQueue<RebalanceSlotInfo> requestQueue = new LinkedBlockingQueue<>(100);
    private RebalanceSlotInfo rebalanceSlotInfo = null;


    public RebalanceManager(LocalController localController) {
        this.localController = localController;
        NamedThreadFactory threadFactory = new NamedThreadFactory("RebalanceManager-");
        Thread thread = threadFactory.newThread(new RebalanceTask());
        thread.start();
    }

    /**
     * 添加一个重平衡任务
     *
     * @param rebalanceSlotInfo 重平衡任务
     * @throws InterruptedException 异常
     */
    public void add(RebalanceSlotInfo rebalanceSlotInfo) throws InterruptedException {
        requestQueue.put(rebalanceSlotInfo);
    }

    /**
     * 负责重平衡的任务
     */
    private class RebalanceTask implements Runnable {
        @Override
        public void run() {
            RebalanceSlotInfo slotInfo;
            while (true) {
                try {
                    slotInfo = requestQueue.poll(10, TimeUnit.MILLISECONDS);
                    if (slotInfo == null) {
                        continue;
                    }
                    if (rebalanceSlotInfo != null && rebalanceSlotInfo.getNodeIdList().contains(slotInfo.getApplyRebalanceNodeId())) {
                        log.warn("上次重平衡的时候，已经把该节点考虑进去了，不需要再处理该节点的重平衡请求：[needRebalanceNodeId={}]",
                                slotInfo.getApplyRebalanceNodeId());
                        continue;
                    }
                    rebalanceSlotInfo = slotInfo;
                    log.info("开始执行节点的重平衡请求：[nodeId={}]", rebalanceSlotInfo.getApplyRebalanceNodeId());
                    Map<Integer, Set<Integer>> nodeSlotsMapSnapshot = rebalanceSlotInfo.getNodeSlotsMapSnapshot();
                    Map<Integer, Integer> slotNodeMapSnapshot = rebalanceSlotInfo.getSlotNodeMapSnapshot();
                    List<Integer> nodeIdList = rebalanceSlotInfo.getNodeIdList();
                    Set<Integer> rebalanceNodes = rebalance(slotNodeMapSnapshot, nodeSlotsMapSnapshot, nodeIdList);
                    rebalanceSlotInfo.setRebalanceNodeIdSet(rebalanceNodes);
                    localController.setRebalanceInfo(rebalanceSlotInfo);
                    rebalanceSlotInfo.waitRemoveMetadataCompleted(rebalanceNodes);
                    log.info("完成了一轮重平衡：[needRebalanceId={}]", rebalanceSlotInfo.getApplyRebalanceNodeId());
                } catch (Exception e) {
                    log.info("RebalanceTask error.", e);
                }
            }
        }
    }

    /**
     * <pre>
     * 用一个简单的算法来进行slot迁移
     *
     * 假设总共slot为16384个，一开始有3个节点：
     *
     *   node1: 5462个slot
     *   node2: 5461个slot
     *   node3: 5461个slot
     *
     * 现在要扩容到5个节点，经过计算得出：
     *
     *   每个节点应该是3276个slot，
     *
     *   需要迁移的slot数量为：3276 * 2(新增节点数) = 6552
     *
     *   其中每个旧节点需要迁移slot数量为： 6552 / 3(旧节点数量) = 2184
     *
     *   则： node04从node01节点获取2184个slot，接着从node02节点获取1092个slot，总共是3276个slot
     *        node05从node02节点获取1092个slot，接着从node03节点获取2184个slot，总共是3276个slot
     *
     * 所以最终结果是这样的：
     *
     *  node1: 3278个slot节点
     *  node2: 3277个slot节点
     *  node3: 3277个slot节点
     *  node4: 3276个slot节点
     *  node5: 3276个slot节点
     * </pre>
     */
    private Set<Integer> rebalance(Map<Integer, Integer> slotNodeMapSnapshot, Map<Integer,
            Set<Integer>> nodeSlotsMapSnapshot, List<Integer> nodeIdList) {
        List<Integer> needAddSlotNodes = new ArrayList<>();
        List<Integer> needRemoveNodes = new ArrayList<>();
        for (Integer nodeId : nodeIdList) {
            Set<Integer> nodeSlots = nodeSlotsMapSnapshot.get(nodeId);
            if (nodeSlots == null || nodeSlots.isEmpty()) {
                needAddSlotNodes.add(nodeId);
            } else {
                needRemoveNodes.add(nodeId);
            }
        }

        int totalNode = nodeIdList.size();
        // 每个节点的slot数量
        int peerNodeSlotCount = Constants.SLOTS_COUNT / totalNode;

        // 需要移动的节点数量
        int needMoveSlotCount = (needAddSlotNodes.size() * peerNodeSlotCount);

        // 每个旧节点需要迁移slot数量
        int perOldNodeRemoveSlotCount = needMoveSlotCount / needRemoveNodes.size();
        int currentNodeRemoveCount = 0;
        int currentRemoveNodeIndex = 0;
        int currentMoveSlotCount = 0;
        for (Integer needAddSlotNode : needAddSlotNodes) {
            Integer needRemoveNodeId = needRemoveNodes.get(currentRemoveNodeIndex);
            Set<Integer> needMoveNodeSlots = nodeSlotsMapSnapshot.get(needRemoveNodeId);
            Iterator<Integer> iterator = needMoveNodeSlots.iterator();
            for (int j = 0; j < peerNodeSlotCount; j++) {
                Integer slot = iterator.next();
                slotNodeMapSnapshot.put(slot, needAddSlotNode);
                Set<Integer> slots = nodeSlotsMapSnapshot.computeIfAbsent(needAddSlotNode, k -> new HashSet<>());
                slots.add(slot);
                iterator.remove();
                currentNodeRemoveCount++;
                currentMoveSlotCount++;
                if (currentNodeRemoveCount >= perOldNodeRemoveSlotCount && currentMoveSlotCount < needMoveSlotCount) {
                    // 当前一个旧节点把应该迁移的slot用完了，但是还不够分配给一个新节点，此时跳到下一个旧节点
                    currentRemoveNodeIndex++;
                    needRemoveNodeId = needRemoveNodes.get(currentRemoveNodeIndex);
                    needMoveNodeSlots = nodeSlotsMapSnapshot.get(needRemoveNodeId);
                    // 由于本身set是无序的，所以这里直接给随机分配即可
                    iterator = needMoveNodeSlots.iterator();
                    currentNodeRemoveCount = 0;
                }
            }
        }
        log.info("重平衡计算结束,结果是: [needAddSlotNodes={}, result={}]", needAddSlotNodes, getString(nodeSlotsMapSnapshot));
        return new HashSet<>(needAddSlotNodes);
    }

    private String getString(Map<Integer, Set<Integer>> nodeSlotsMapSnapshot) {
        StringBuilder sb = new StringBuilder();
        int size = nodeSlotsMapSnapshot.size();
        int i = 0;
        for (Map.Entry<Integer, Set<Integer>> entry : nodeSlotsMapSnapshot.entrySet()) {
            sb.append("{nodeId=")
                    .append(entry.getKey())
                    .append(", slots=")
                    .append(entry.getValue().size())
                    .append("}");
            if (i < size - 1) {
                sb.append(", ");
            }
            i++;
        }
        return sb.toString();
    }

}
