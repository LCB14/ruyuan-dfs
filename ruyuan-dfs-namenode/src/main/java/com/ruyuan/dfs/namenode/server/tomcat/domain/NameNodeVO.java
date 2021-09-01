package com.ruyuan.dfs.namenode.server.tomcat.domain;

import com.ruyuan.dfs.model.namenode.NameNodeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * NameNode节点信息
 *
 * @author Sun Dasheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class NameNodeVO {

    private Integer nodeId;
    private String hostname;
    private Integer httpPort;
    private Integer nioPort;
    private String backupNodeInfo;

    public static NameNodeVO parse(NameNodeInfo nameNodeInfo) {
        NameNodeVO vo = new NameNodeVO();
        vo.setNodeId(nameNodeInfo.getNodeId());
        vo.setHostname(nameNodeInfo.getHostname());
        vo.setHttpPort(nameNodeInfo.getHttpPort());
        vo.setNioPort(nameNodeInfo.getNioPort());
        vo.setBackupNodeInfo(nameNodeInfo.getBackupNodeInfo());
        return vo;
    }
}
