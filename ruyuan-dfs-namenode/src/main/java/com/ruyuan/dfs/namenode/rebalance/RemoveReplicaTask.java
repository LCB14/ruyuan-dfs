package com.ruyuan.dfs.namenode.rebalance;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 移除副本任务
 *
 * @author Sun Dasheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RemoveReplicaTask {

    private String hostname;
    private String fileName;
}

