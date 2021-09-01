package com.ruyuan.dfs.namenode.server.tomcat.servlet;

import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.enums.NameNodeLaunchMode;
import com.ruyuan.dfs.common.enums.PacketType;
import com.ruyuan.dfs.common.metrics.Prometheus;
import com.ruyuan.dfs.model.client.GetDataNodeForFileRequest;
import com.ruyuan.dfs.model.client.GetDataNodeForFileResponse;
import com.ruyuan.dfs.model.common.DataNode;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.datanode.DataNodeInfo;
import com.ruyuan.dfs.namenode.datanode.DataNodeManager;
import com.ruyuan.dfs.namenode.shard.ShardingManager;
import com.ruyuan.dfs.namenode.shard.peer.PeerNameNodes;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLDecoder;

/**
 * 文件下载Servlet
 *
 * @author Sun Dasheng
 */
@Slf4j
public class FileDownloadServlet extends javax.servlet.http.HttpServlet {

    private NameNodeConfig nameNodeConfig;
    private ShardingManager shardingManager;
    private PeerNameNodes peerNameNodes;
    private DataNodeManager dataNodeManager;
    private boolean isClusterMode;

    public FileDownloadServlet(NameNodeConfig nameNodeConfig, DataNodeManager dataNodeManager,
                               PeerNameNodes peerNameNodes, ShardingManager shardingManager) {
        this.nameNodeConfig = nameNodeConfig;
        this.dataNodeManager = dataNodeManager;
        this.peerNameNodes = peerNameNodes;
        this.shardingManager = shardingManager;
        this.isClusterMode = NameNodeLaunchMode.CLUSTER.equals(nameNodeConfig.getMode());
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        // 首先根据文件名，定位到改文件所属的NameNode节点，然后转发过去具体的Namenode获取DataNode的信息。
        String requestUri = req.getRequestURI();
        String filename = URLDecoder.decode(requestUri, "UTF-8");
        if (!"true".equals(req.getHeader("ruyuan-dfs-http—get-file"))) {
            log.warn("收到异常请求，拒绝：{}", requestUri);
            resp.sendError(403, "Get out of here.");
            return;
        }
        if (isClusterMode) {
            int nodeId = shardingManager.getNameNodeIdByFileName(filename);
            if (nodeId == nameNodeConfig.getNameNodeId()) {
                redirectToDataNode(resp, requestUri, filename);
                return;
            }
            GetDataNodeForFileRequest request = GetDataNodeForFileRequest.newBuilder()
                    .setFilename(filename)
                    .build();
            NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.GET_DATA_NODE_FOR_FILE);
            NettyPacket fileResp;
            try {
                fileResp = peerNameNodes.sendSync(nodeId, nettyPacket);
            } catch (Exception e) {
                log.error("下载文件失败：", e);
                resp.sendError(500, "File Failed: " + filename);
                return;
            }
            if (fileResp.isError()) {
                resp.sendError(404, "File Not Found: " + filename);
                return;
            }
            GetDataNodeForFileResponse response = GetDataNodeForFileResponse.parseFrom(fileResp.getBody());
            DataNode dataNode = response.getDataNode();
            String redirectUrl = "http://" + dataNode.getHostname() + ":" + dataNode.getHttpPort() + requestUri;
            Prometheus.incCounter("namenode_http_get_file_count", "NameNode收到的HTTP下载文件请求数量");
            if (log.isDebugEnabled()) {
                log.debug("重定向DataNode节点，进行文件下载: [url={}]", redirectUrl);
            }
        } else {
            redirectToDataNode(resp, requestUri, filename);
        }
    }

    private void redirectToDataNode(HttpServletResponse resp, String requestUri, String filename) throws IOException {
        DataNodeInfo dataNode = dataNodeManager.chooseReadableDataNodeByFileName(filename);
        if (dataNode == null) {
            resp.sendError(404, "File Not Found: " + filename);
            return;
        }
        String redirectUrl = "http://" + dataNode.getHostname() + ":" + dataNode.getHttpPort() + requestUri;
        if (log.isDebugEnabled()) {
            log.debug("重定向DataNode节点，进行文件下载: [url={}]", redirectUrl);
        }
        resp.sendRedirect(redirectUrl);
    }
}
