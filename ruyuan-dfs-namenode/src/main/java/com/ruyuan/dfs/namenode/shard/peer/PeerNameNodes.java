package com.ruyuan.dfs.namenode.shard.peer;

import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.exception.RequestTimeoutException;
import com.ruyuan.dfs.common.network.NetClient;
import com.ruyuan.dfs.common.utils.DefaultScheduler;
import com.ruyuan.dfs.ha.NodeRoleSwitcher;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.server.NameNodeApis;
import com.ruyuan.dfs.namenode.shard.controller.ControllerManager;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * 负责维护所有NameNode节点之间的组件
 *
 * @author Sun Dasheng
 */
@Slf4j
public class PeerNameNodes {

    private ControllerManager controllerManager;
    private final NameNodeConfig nameNodeConfig;
    private final DefaultScheduler defaultScheduler;
    private final Map<Integer, PeerNameNode> peerNameNodeMap = new ConcurrentHashMap<>();
    private NameNodeApis nameNodeApis;

    public PeerNameNodes(DefaultScheduler defaultScheduler, NameNodeConfig nameNodeConfig) {
        this.defaultScheduler = defaultScheduler;
        this.nameNodeConfig = nameNodeConfig;
    }

    public void setControllerManager(ControllerManager controllerManager) {
        this.controllerManager = controllerManager;
    }

    public void setNameNodeApis(NameNodeApis nameNodeApis) {
        this.nameNodeApis = nameNodeApis;
    }

    /**
     * 作为客户端添加PeerNode: 主动连接NameNode节点
     *
     * @param server 节点信息
     */
    public void connect(String server) {
        connect(server, false);
    }

    /**
     * 作为客户端添加PeerNode: 主动连接NameNode节点
     *
     * @param server 节点信息
     * @param force  是否强制替换链接
     */
    public void connect(String server, boolean force) {
        String[] info = server.split(":");
        String hostname = info[0];
        int port = Integer.parseInt(info[1]);
        int targetNodeId = Integer.parseInt(info[2]);
        if (targetNodeId == nameNodeConfig.getNameNodeId() && port == nameNodeConfig.getPort()) {
            return;
        }
        boolean isUpgradeFromBackup = NodeRoleSwitcher.getInstance().isUpgradeFromBackup();
        // 如果是BackupNode升级的，需要往比他大的id发送连接
        if (!isUpgradeFromBackup && targetNodeId > nameNodeConfig.getNameNodeId()) {
            return;
        }
        synchronized (this) {
            String peerDataNode = hostname + ":" + port + ":" + targetNodeId;
            PeerNameNode peer = peerNameNodeMap.get(targetNodeId);
            if (force || peer == null) {
                if (peer != null) {
                    peer.close();
                }
                NetClient netClient = new NetClient("NameNode-PeerNode-" + targetNodeId, defaultScheduler);
                netClient.addHandlers(Collections.singletonList(nameNodeApis));
                PeerNameNode newPeer = new PeerNameNodeClient(netClient, nameNodeConfig.getNameNodeId(), targetNodeId, peerDataNode);
                peerNameNodeMap.put(targetNodeId, newPeer);
                netClient.addConnectListener(connected -> {
                    if (connected) {
                        controllerManager.reportSelfInfoToPeer(newPeer, true);
                    }
                });
                netClient.connect(hostname, port);
                log.info("新建PeerNameNode的链接：[hostname={}, port={}, nameNodeId={}]", hostname, port, targetNodeId);
            }
        }
    }

    /**
     * 作为服务端添加PeerNode: 收到其他NameNode节点的请求
     *
     * @param nameNodeId       NameNode节点ID
     * @param channel          连接
     * @param server           PeerNode的ip和端口号
     * @param defaultScheduler 调度器
     * @return 是否产生新的连接
     */
    public PeerNameNode addPeerNode(int nameNodeId, SocketChannel channel, String server,
                                    int selfNameNodeId, DefaultScheduler defaultScheduler) {
        synchronized (this) {
            PeerNameNode oldPeer = peerNameNodeMap.get(nameNodeId);
            PeerNameNode newPeer = new PeerNameNodeServer(channel, nameNodeConfig.getNameNodeId(), nameNodeId, server, defaultScheduler);
            if (oldPeer == null) {
                log.info("收到新的PeerNameNode的通知网络包, 保存连接以便下一次使用: [nodeId={}]", nameNodeId);
                peerNameNodeMap.put(nameNodeId, newPeer);
                return newPeer;
            } else {
                if (oldPeer instanceof PeerNameNodeServer && newPeer.getTargetNodeId() == oldPeer.getTargetNodeId()) {
                    // 此种情况为断线重连, 需要更新channel
                    PeerNameNodeServer peerNameNodeServer = (PeerNameNodeServer) oldPeer;
                    peerNameNodeServer.setSocketChannel(channel);
                    log.info("PeerNameNode断线重连，更新channel: [nodeId={}]", oldPeer.getTargetNodeId());
                    return oldPeer;
                } else {
                    /*
                     * 考虑这样一种情况：
                     *    namenode01 -> backupnode01
                     *    namenode02
                     *    namenode03
                     *
                     * 当namenode01挂了，backupnode01顶上，此时backupnode01作为id比较小的会往namenode02发起连接。
                     *
                     * 则对于namenode02来说: 自身id=2，收到了id=1的客户端发起的链接，此时需要关闭的链接为:
                     *    1、 namenode02   -> namenode01的旧链接
                     *    2、 backupnode01 -> namenode02的新的连接
                     *
                     * 关闭连接后，再由namenode02主动往backupnode01发起连接，保持整个集群的连接情况是id较大的节点主动往id较小的节点发起连接
                     *
                     * 对于backupnode01来说：收到namenode02主动发起的链接后. 则应该关闭的链接为：
                     *    1、backupnode01 -> namenode02的旧的连接
                     *
                     */

                    if (selfNameNodeId > nameNodeId) {
                        newPeer.close();
                        connect(server, true);
                        log.info("新的连接NameNodeId比较小，关闭新的连接, 并主动往小id的节点发起连接: [nodeId={}]", newPeer.getTargetNodeId());
                        return null;
                    } else {
                        // 出现在BackupNode升级为NameNode后，主动往比自己id大的NameNode发起连接，接着NameNode节点再次往BackupNode发起连接的时候，BackupNode代码走到这里
                        peerNameNodeMap.put(nameNodeId, newPeer);
                        oldPeer.close();
                        log.info("新的连接NameNodeId比较大，则关闭旧的连接, 并替换链接: [nodeId={}]", oldPeer.getTargetNodeId());
                        return newPeer;
                    }
                }
            }
        }
    }

    /**
     * 广播发给所有的NameNode
     *
     * @param nettyPacket 包
     */
    public List<Integer> broadcast(NettyPacket nettyPacket) {
        return broadcast(nettyPacket, -1);
    }


    /**
     * 广播发给所有的NameNode
     *
     * @param nettyPacket  包
     * @param excludeNodeId 不给该Node发送请求
     */
    public List<Integer> broadcast(NettyPacket nettyPacket, int excludeNodeId) {
        return broadcast(nettyPacket, new HashSet<>(Collections.singletonList(excludeNodeId)));
    }


    /**
     * 广播发给所有的NameNode
     *
     * @param nettyPacket   包
     * @param excludeNodeIds 不给该Node发送请求
     */
    public List<Integer> broadcast(NettyPacket nettyPacket, Set<Integer> excludeNodeIds) {
        try {
            List<Integer> result = new ArrayList<>();
            for (PeerNameNode peerNameNode : peerNameNodeMap.values()) {
                if (excludeNodeIds.contains(peerNameNode.getTargetNodeId())) {
                    continue;
                }
                peerNameNode.send(nettyPacket);
                result.add(peerNameNode.getTargetNodeId());
            }
            return result;
        } catch (Exception e) {
            log.error("PeerNameNodes#boardcast has interrupted. ", e);
            return new ArrayList<>();
        }
    }

    /**
     * 广播发给所有的NameNode,同步获取结果
     *
     * @param request 包
     */
    public List<NettyPacket> broadcastSync(NettyPacket request) {
        try {
            if (peerNameNodeMap.size() == 0) {
                return new ArrayList<>();
            }
            List<NettyPacket> result = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(peerNameNodeMap.size());
            for (PeerNameNode peerNameNode : peerNameNodeMap.values()) {
                defaultScheduler.scheduleOnce("同步请求PeerNameNode", () -> {
                    NettyPacket response;
                    NettyPacket requestCopy = NettyPacket.copy(request);
                    try {
                        response = peerNameNode.sendSync(requestCopy);
                        result.add(response);
                    } catch (Exception e) {
                        log.error("同步请求peerNode失败，sequence=" + requestCopy.getSequence(), e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await();
            return result;
        } catch (Exception e) {
            log.error("PeerNameNodes#boardcast has interrupted. ", e);
            return new ArrayList<>();
        }
    }


    /**
     * 优雅停止
     */
    public void shutdown() {
        log.info("Shutdown PeerNameNodes");
        for (PeerNameNode peerNameNode : peerNameNodeMap.values()) {
            peerNameNode.close();
        }
    }

    public List<String> getAllServers() {
        List<String> result = new ArrayList<>(peerNameNodeMap.size());
        for (PeerNameNode peerNameNode : peerNameNodeMap.values()) {
            result.add(peerNameNode.getServer());
        }
        return result;
    }

    public List<Integer> getAllNodeId() {
        return new ArrayList<>(peerNameNodeMap.keySet());
    }

    /**
     * 获取所有已经建立连接的节点数量
     *
     * @return 建立连接的节点数量
     */
    public int getConnectedCount() {
        synchronized (this) {
            int count = 0;
            for (PeerNameNode peerNameNode : peerNameNodeMap.values()) {
                if (peerNameNode.isConnected()) {
                    count++;
                }
            }
            return count;
        }
    }

    /**
     * 收到消息，看看消息是否先被 PeerNameNodeServer消费
     *
     * @param request 消息
     * @return 是否被消费
     */
    public boolean onMessage(NettyPacket request) {
        PeerNameNode peerNameNode = peerNameNodeMap.get(request.getNodeId());
        if (peerNameNode == null) {
            return false;
        }
        if (!(peerNameNode instanceof PeerNameNodeServer)) {
            return false;
        }
        PeerNameNodeServer server = (PeerNameNodeServer) peerNameNode;
        return server.onMessage(request);
    }

    /**
     * 往其中一个节点发送请求
     *
     * @param nameNodeId   节点Id
     * @param nettyPacket 请求
     * @throws InterruptedException 异常
     */
    public void send(int nameNodeId, NettyPacket nettyPacket) throws InterruptedException {
        PeerNameNode peerNameNode = peerNameNodeMap.get(nameNodeId);
        if (peerNameNode != null) {
            peerNameNode.send(nettyPacket);
        } else {
            log.warn("找不到Peer节点：[nodeId={}]", nameNodeId);
        }
    }

    /**
     * 往其中一个节点发送请求
     *
     * @param nameNodeId   节点Id
     * @param nettyPacket 请求
     * @throws InterruptedException 异常
     */
    public NettyPacket sendSync(int nameNodeId, NettyPacket nettyPacket) throws InterruptedException, RequestTimeoutException {
        PeerNameNode peerNameNode = peerNameNodeMap.get(nameNodeId);
        if (peerNameNode != null) {
            return peerNameNode.sendSync(nettyPacket);
        } else {
            log.warn("找不到Peer节点：[nodeId={}]", nameNodeId);
        }
        throw new IllegalArgumentException("Invalid nodeId: " + nameNodeId);
    }

}
