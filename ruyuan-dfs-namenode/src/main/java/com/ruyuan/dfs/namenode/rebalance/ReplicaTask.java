package com.ruyuan.dfs.namenode.rebalance;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 副本复制任务
 *
 * @author Sun Dasheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReplicaTask {

    private String filename;
    private String hostname;
    private int port;

}
