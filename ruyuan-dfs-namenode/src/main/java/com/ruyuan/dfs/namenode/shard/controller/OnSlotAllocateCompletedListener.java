package com.ruyuan.dfs.namenode.shard.controller;

import java.util.Map;

/**
 * Slot 分配成功的监听器
 *
 * @author Sun Dasheng
 */
public interface OnSlotAllocateCompletedListener {

    /**
     * 节点槽位分配完成
     *
     * @param slots 槽位
     */
    void onSlotAllocateCompleted(Map<Integer, Integer> slots);

}
