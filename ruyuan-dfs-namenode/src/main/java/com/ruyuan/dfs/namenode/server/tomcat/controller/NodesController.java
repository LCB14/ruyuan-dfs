package com.ruyuan.dfs.namenode.server.tomcat.controller;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.enums.PacketType;
import com.ruyuan.dfs.common.exception.RequestTimeoutException;
import com.ruyuan.dfs.common.utils.NetUtils;
import com.ruyuan.dfs.model.backup.BackupNodeInfo;
import com.ruyuan.dfs.model.common.DataNode;
import com.ruyuan.dfs.model.namenode.AddReplicaNumRequest;
import com.ruyuan.dfs.model.namenode.FetchDataNodeByFilenameRequest;
import com.ruyuan.dfs.model.namenode.FetchDataNodeByFilenameResponse;
import com.ruyuan.dfs.model.namenode.NameNodeInfo;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.datanode.DataNodeInfo;
import com.ruyuan.dfs.namenode.datanode.DataNodeManager;
import com.ruyuan.dfs.namenode.server.BackupNodeInfoHolder;
import com.ruyuan.dfs.namenode.server.NameNodeApis;
import com.ruyuan.dfs.namenode.server.tomcat.annotation.Autowired;
import com.ruyuan.dfs.namenode.server.tomcat.annotation.RequestMapping;
import com.ruyuan.dfs.namenode.server.tomcat.annotation.RestController;
import com.ruyuan.dfs.namenode.server.tomcat.domain.*;
import com.ruyuan.dfs.namenode.shard.peer.PeerNameNodes;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 节点信息Controller
 *
 * @author Sun Dasheng
 */
@RestController
@RequestMapping("/api/nodes")
public class NodesController {

    @Autowired
    private DataNodeManager dataNodeManager;

    @Autowired
    private NameNodeConfig nameNodeConfig;

    @Autowired
    private NameNodeApis nameNodeApis;

    @Autowired
    private PeerNameNodes peerNameNodes;

    @RequestMapping("/datanodes")
    public CommonResponse<List<DataNodeVO>> getDataNodeInfo() {
        List<DataNodeInfo> dataNodeInfoList = dataNodeManager.getDataNodeInfoList();
        List<DataNodeVO> vos = DataNodeVO.parse(dataNodeInfoList, nameNodeConfig.getDataNodeHeartbeatTimeout());
        return CommonResponse.successWith(vos);
    }

    @RequestMapping("/namenodes")
    public CommonResponse<List<NameNodeVO>> getNameNodeInfo() throws InvalidProtocolBufferException {
        NameNodeVO nameNodeVO = new NameNodeVO();
        nameNodeVO.setHostname(NetUtils.getHostName());
        nameNodeVO.setNodeId(nameNodeConfig.getNameNodeId());
        nameNodeVO.setHttpPort(nameNodeConfig.getHttpPort());
        BackupNodeInfoHolder backupNodeInfoHolder = nameNodeApis.getBackupNodeInfoHolder();
        String backupNodeInfo = "";
        if (backupNodeInfoHolder != null) {
            BackupNodeInfo backupNode = backupNodeInfoHolder.getBackupNodeInfo();
            backupNodeInfo = backupNode.getHostname() + ":" + backupNode.getPort();
        }
        nameNodeVO.setBackupNodeInfo(backupNodeInfo);
        nameNodeVO.setNioPort(nameNodeConfig.getPort());
        List<NameNodeVO> result = new ArrayList<>();
        result.add(nameNodeVO);
        NettyPacket nettyPacket = NettyPacket.buildPacket(new byte[0], PacketType.FETCH_NAME_NODE_INFO);
        List<NettyPacket> nettyPackets = peerNameNodes.broadcastSync(nettyPacket);
        for (NettyPacket response : nettyPackets) {
            NameNodeInfo nameNodeInfo = NameNodeInfo.parseFrom(response.getBody());
            result.add(NameNodeVO.parse(nameNodeInfo));
        }
        return CommonResponse.successWith(result);
    }


    @RequestMapping("/getFileStorageInfo")
    public CommonResponse<List<DataNodeVO>> getFileStorageInfo(UserFilesQuery userFilesQuery) throws RequestTimeoutException,
            InterruptedException, InvalidProtocolBufferException {
        String realFilename = File.separator + userFilesQuery.getUsername() + userFilesQuery.getPath();
        List<DataNodeInfo> dataNodeByFileName;
        int nodeId = nameNodeApis.getNodeId(realFilename);
        if (nameNodeConfig.getNameNodeId() == nodeId) {
            dataNodeByFileName = dataNodeManager.getDataNodeByFileName(realFilename);
        } else {
            FetchDataNodeByFilenameRequest request = FetchDataNodeByFilenameRequest.newBuilder()
                    .setFilename(userFilesQuery.getPath())
                    .build();
            NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.FETCH_DATA_NODE_BY_FILENAME);
            nettyPacket.setUsername(userFilesQuery.getUsername());
            NettyPacket responsePackage = peerNameNodes.sendSync(nodeId, nettyPacket);
            FetchDataNodeByFilenameResponse response = FetchDataNodeByFilenameResponse.parseFrom(responsePackage.getBody());
            dataNodeByFileName = new ArrayList<>();
            for (DataNode dataNode : response.getDatanodesList()) {
                dataNodeByFileName.add(dataNodeManager.getDataNode(dataNode.getHostname()));
            }
        }
        List<DataNodeVO> vos = DataNodeVO.parse(dataNodeByFileName, nameNodeConfig.getDataNodeHeartbeatTimeout());
        return CommonResponse.successWith(vos);
    }

    @RequestMapping("/changeReplicaNum")
    public CommonResponse<Boolean> changeReplicaNum(ChangeReplicaNumVO changeReplicaNumVO) throws Exception {
        String fullPath = File.separator + changeReplicaNumVO.getUsername() + changeReplicaNumVO.getPath();
        int nodeId = nameNodeApis.getNodeId(fullPath);
        if (nameNodeConfig.getNameNodeId() == nodeId) {
            List<DataNodeInfo> dataNodeByFileName = dataNodeManager.getDataNodeByFileName(fullPath);
            int addReplicaNum = changeReplicaNumVO.getReplicaNum() - dataNodeByFileName.size();
            if (addReplicaNum < 0) {
                return CommonResponse.failWith("修改失败，不能动态减少副本数量");
            } else if (addReplicaNum == 0) {
                return CommonResponse.successWith(true);
            } else {
                dataNodeManager.addReplicaNum(changeReplicaNumVO.getUsername(),
                        addReplicaNum, fullPath);
                return CommonResponse.successWith(true);
            }
        } else {
            AddReplicaNumRequest request = AddReplicaNumRequest.newBuilder()
                    .setFilename(changeReplicaNumVO.getPath())
                    .setReplicaNum(changeReplicaNumVO.getReplicaNum())
                    .build();
            NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.ADD_REPLICA_NUM);
            nettyPacket.setUsername(changeReplicaNumVO.getUsername());
            NettyPacket responsePackage = peerNameNodes.sendSync(nodeId, nettyPacket);
            if (responsePackage.isSuccess()) {
                return CommonResponse.successWith(true);
            } else {
                return CommonResponse.failWith(responsePackage.getError());
            }
        }
    }
}
