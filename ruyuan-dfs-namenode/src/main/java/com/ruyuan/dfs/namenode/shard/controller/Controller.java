package com.ruyuan.dfs.namenode.shard.controller;

import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.network.RequestWrapper;
import com.ruyuan.dfs.model.namenode.NameNodeSlots;

import java.util.Map;

/**
 * 负责处理元数据分片的组件
 *
 * @author Sun Dasheng
 */
public interface Controller {

    /**
     * 初始化槽位分配
     *
     * <pre>
     *   对于Controller节点：执行平衡分配Slot
     *   对于一般节点：等待Controller分配好Slot，并把Slot信息发送给我
     * <pre/>
     * @return Slot分配信息
     * @throws Exception 异常
     */
    Map<Integer, Integer> initSlotAllocate() throws Exception;

    /**
     * 收到Slot信息
     *
     * @param nameNodeSlots slots信息
     * @throws Exception 异常
     */
    default void onReceiveSlots(NameNodeSlots nameNodeSlots) throws Exception {
        // 默认啥操作都没有
    }

    /**
     * Slots重平衡操作
     *
     * <pre>
     *   对于Controller节点：执行重平衡分配Slot
     *   对于一般节点：发送请求给Controller要求重平衡分配Slot
     * <pre/>
     * @param nettyPacket 网络包
     * @throws Exception 异常
     */
    void rebalanceSlots(NettyPacket nettyPacket) throws Exception;

    /**
     * 需要重平衡的节点抓取到其他节点的元数据信息
     *
     * @param requestWrapper 请求
     * @throws Exception 异常
     */
    default void onFetchMetadata(RequestWrapper requestWrapper) throws Exception {

    }

    /**
     * 让各个需要重平衡的节点删除自己的元数据，并让新的slot分配信息生效
     *
     * <pre>
     *   对于Controller节点：收到的是需要重平衡的节点，需要移除内存元数据，并发送广播给所有其他的NameNode，让它们也移除元数据
     *   对于一般节点：收到的是Controller的广播信息，需要移除内存元数据，并上报给Controller
     * <pre/>
     *
     * @throws Exception 中断异常
     * @param rebalanceNodeId 重平衡请求发起的节点
     */
    void onFetchSlotMetadataCompleted(int rebalanceNodeId) throws Exception;

    /**
     * Controller节点收到其他NameNode节点删除内存元数据的上报请求
     *
     * @param nettyPacket 请求
     * @throws Exception 序列化异常
     */
    default void onRemoveMetadataCompleted(NettyPacket nettyPacket) throws Exception {

    }

    /**
     * 根据Slot返回节点id
     *
     * @param slot slot槽位
     * @return 节点ID
     */
    int getNodeIdBySlot(int slot);

    /**
     * 添加slot分配完成的监听器
     *
     * @param listener 监听器
     */
    void addOnSlotAllocateCompletedListener(OnSlotAllocateCompletedListener listener);

    /**
     * 获取Slot信息
     *
     * @return slot信息
     */
    Map<Integer, Integer> getSlotNodeMap();
}
