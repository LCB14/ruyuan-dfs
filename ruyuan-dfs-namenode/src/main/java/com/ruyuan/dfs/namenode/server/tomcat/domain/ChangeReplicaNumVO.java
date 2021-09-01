package com.ruyuan.dfs.namenode.server.tomcat.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 修改文件副本请求
 *
 * @author Sun Dasheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChangeReplicaNumVO {
    private String path;
    private String username;
    private Integer replicaNum;
}
