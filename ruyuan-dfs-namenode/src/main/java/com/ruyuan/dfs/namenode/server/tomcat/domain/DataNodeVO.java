package com.ruyuan.dfs.namenode.server.tomcat.domain;

import com.ruyuan.dfs.common.utils.DateUtils;
import com.ruyuan.dfs.common.utils.FileUtil;
import com.ruyuan.dfs.common.utils.StringUtils;
import com.ruyuan.dfs.namenode.datanode.DataNodeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 数据节点信息
 *
 * @author Sun Dasheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataNodeVO {

    private Integer nodeId;
    private String hostname;
    private Integer httpPort;
    private Integer nioPort;
    private String latestHeartbeatTime;
    private String storedDataSize;
    private String freeSpace;
    private String status;
    private String usePercent;

    public static List<DataNodeVO> parse(List<DataNodeInfo> dataNodeInfoList, long heartbeatDelta) {
        if (dataNodeInfoList == null) {
            return null;
        }
        List<DataNodeVO> result = new ArrayList<>();
        for (DataNodeInfo dataNodeInfo : dataNodeInfoList) {
            DataNodeVO vo = new DataNodeVO();
            vo.setNodeId(dataNodeInfo.getNodeId());
            vo.setHostname(dataNodeInfo.getHostname());
            vo.setHttpPort(dataNodeInfo.getHttpPort());
            vo.setNioPort(dataNodeInfo.getNioPort());
            vo.setLatestHeartbeatTime(DateUtils.format(new Date(dataNodeInfo.getLatestHeartbeatTime() - heartbeatDelta)));
            vo.setStoredDataSize(FileUtil.formatSize(dataNodeInfo.getStoredDataSize()));
            vo.setFreeSpace(FileUtil.formatSize(dataNodeInfo.getFreeSpace()));
            vo.setStatus(DataNodeInfo.STATUS_INIT == dataNodeInfo.getStatus() ? "未就绪" : "就绪");
            vo.setUsePercent(StringUtils.getPercent(dataNodeInfo.getStoredDataSize() + dataNodeInfo.getFreeSpace(),
                    dataNodeInfo.getStoredDataSize()));
            result.add(vo);
        }
        return result;
    }
}
