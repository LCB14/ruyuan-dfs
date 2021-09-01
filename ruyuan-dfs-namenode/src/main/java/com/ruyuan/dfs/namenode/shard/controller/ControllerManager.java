package com.ruyuan.dfs.namenode.shard.controller;

import com.ruyuan.dfs.common.FileInfo;
import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.enums.PacketType;
import com.ruyuan.dfs.common.network.RequestWrapper;
import com.ruyuan.dfs.common.utils.DefaultScheduler;
import com.ruyuan.dfs.common.utils.NetUtils;
import com.ruyuan.dfs.model.namenode.*;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.datanode.DataNodeManager;
import com.ruyuan.dfs.namenode.fs.DiskNameSystem;
import com.ruyuan.dfs.namenode.server.UserManager;
import com.ruyuan.dfs.namenode.shard.peer.PeerNameNode;
import com.ruyuan.dfs.namenode.shard.peer.PeerNameNodes;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Controller选举组件
 *
 * @author Sun Dasheng
 */
@Slf4j
public class ControllerManager {

    private UserManager userManager;
    private DataNodeManager dataNodeManager;
    private DiskNameSystem diskNameSystem;
    private DefaultScheduler defaultScheduler;
    private PeerNameNodes peerNameNodes;
    private AtomicInteger numOfNode;
    private NameNodeConfig nameNodeConfig;
    private ControllerVote currentVote;
    private AtomicInteger voteRound = new AtomicInteger(0);
    private List<ControllerVote> voteList = new ArrayList<>();
    private Controller controller;
    private AtomicBoolean startControllerElection = new AtomicBoolean(false);
    private AtomicInteger newNodeCount = new AtomicInteger(0);
    private AtomicBoolean isForeController = new AtomicBoolean(false);
    private List<OnSlotAllocateCompletedListener> slotAllocateCompletedListeners = new ArrayList<>();

    public ControllerManager(NameNodeConfig nameNodeConfig, PeerNameNodes peerNameNodes, DefaultScheduler defaultScheduler,
                             DiskNameSystem diskNameSystem, DataNodeManager dataNodeManager, UserManager userManager) {
        this.nameNodeConfig = nameNodeConfig;
        this.peerNameNodes = peerNameNodes;
        this.defaultScheduler = defaultScheduler;
        this.diskNameSystem = diskNameSystem;
        this.dataNodeManager = dataNodeManager;
        this.numOfNode = new AtomicInteger(nameNodeConfig.numOfNode());
        this.peerNameNodes.setControllerManager(this);
        this.userManager = userManager;
        this.currentVote = ControllerVote.newBuilder()
                .setVoterNodeId(nameNodeConfig.getNameNodeId())
                .setControllerNodeId(nameNodeConfig.getNameNodeId())
                .setVoteRound(voteRound.get())
                .setForce(false)
                .build();
    }

    /**
     * 上报自身信息给其他节点PeerNameNode
     *
     * @param nameNode NameNode
     * @throws InterruptedException 异常
     */
    public void reportSelfInfoToPeer(PeerNameNode nameNode, boolean isClient) throws InterruptedException {
        String hostName = NetUtils.getHostName();
        NameNodeAwareRequest nameNodeInfo = NameNodeAwareRequest.newBuilder()
                .setNameNodeId(nameNodeConfig.getNameNodeId())
                .setServer(hostName + ":" + nameNodeConfig.getPort() + ":" + nameNodeConfig.getNameNodeId())
                .setNumOfNode(numOfNode.get())
                .addAllServers(peerNameNodes.getAllServers())
                .setIsClient(isClient)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(nameNodeInfo.toByteArray(), PacketType.NAME_NODE_PEER_AWARE);
        nameNode.send(nettyPacket);
        log.info("建立了PeerNameNode的连接, 发送自身信息：[currentNodeId={}, targetNodeId={}]", nameNodeConfig.getNameNodeId(),
                nameNode.getTargetNodeId());
    }

    /**
     * 收到了Controller选举的归票
     */
    public void onReceiveControllerVote(RequestWrapper requestWrapper) throws Exception {
        synchronized (this) {
            NettyPacket request = requestWrapper.getRequest();
            ControllerVote controllerVote = ControllerVote.parseFrom(request.getBody());
            log.info("收到Controller选举投票：[voter={}, voteNodeId={}, voteRound={}]", controllerVote.getVoterNodeId(),
                    controllerVote.getControllerNodeId(), controllerVote.getVoteRound());
            if (controller != null && newNodeCount.get() > 0) {
                log.info("集群已经有Controller节点了, 让它强制承认当前集群的Controller：[voter={}, voteNodeId={}, voteRound={}]",
                        currentVote.getVoterNodeId(), currentVote.getControllerNodeId(), currentVote.getVoteRound());
                ControllerVote vote = ControllerVote.newBuilder()
                        .setForce(true)
                        .setVoteRound(currentVote.getVoteRound())
                        .setControllerNodeId(currentVote.getControllerNodeId())
                        .setVoterNodeId(currentVote.getVoterNodeId())
                        .build();
                NettyPacket votePackage = NettyPacket.buildPacket(vote.toByteArray(), PacketType.NAME_NODE_CONTROLLER_VOTE);
                requestWrapper.sendResponse(votePackage, null);
                newNodeCount.decrementAndGet();
            } else {
                voteList.add(controllerVote);
                int quorum = numOfNode.get() / 2 + 1;
                if (voteList.size() >= quorum) {
                    notifyAll();
                }
            }
        }
    }


    /**
     * 收到PeerNameNode节点发过来的信息
     */
    public void onAwarePeerNameNode(NameNodeAwareRequest request) throws Exception {
        newNodeCount.incrementAndGet();
        numOfNode.set(Math.max(numOfNode.get(), request.getNumOfNode()));
        for (String server : request.getServersList()) {
            peerNameNodes.connect(server);
        }
        log.info("收到PeerNameNode发送过来的信息：[nodeId={}, curNumOfNum={}, peerNodeNum={}]",
                request.getNameNodeId(), numOfNode.get(), peerNameNodes.getConnectedCount());
        // 这里需要把自身作为一个节点排除掉。自己不需要和自己建立链接
        if (peerNameNodes.getConnectedCount() == numOfNode.get() - 1) {
            // 因为这里有阻塞方法，回调中不能阻塞，
            // 否则会导致后面网络收发失败，所以需要新开线程处理
            startControllerElection();
        }
    }

    /**
     * 启动Controller选举
     */
    public void startControllerElection() throws Exception {
        if (startControllerElection.compareAndSet(false, true)) {
            voteList.add(currentVote);
            // 发送信息给所有的PeerNameNode节点，开始选举
            NettyPacket nettyPacket = NettyPacket.buildPacket(currentVote.toByteArray(), PacketType.NAME_NODE_CONTROLLER_VOTE);
            List<Integer> nodeIdList = peerNameNodes.broadcast(nettyPacket);
            log.info("开始尝试选举Controller，发送当前选票给所有的节点: [nodes={}]", nodeIdList);

            // 归票选出Controller
            int quorum = numOfNode.get() / 2 + 1;
            Integer controllerId;
            while (true) {
                if (voteList.size() != numOfNode.get()) {
                    synchronized (this) {
                        wait(10);
                    }
                }
                controllerId = getControllerFromVotes(voteList, quorum);
                if (controllerId != null) {
                    log.info("选举出来了Controller: [controllerNodeId={}]", controllerId);
                    initController(controllerId);
                    return;
                } else if (voteList.size() == numOfNode.get()) {
                    log.info("当前选举无法选出Controller, 进行下一轮选举.");
                    break;
                }
            }
            Integer betterControllerNodeId = getBetterControllerNodeId(voteList);
            this.currentVote = ControllerVote.newBuilder()
                    .setVoterNodeId(nameNodeConfig.getNameNodeId())
                    .setControllerNodeId(betterControllerNodeId)
                    .setVoteRound(voteRound.incrementAndGet())
                    .setForce(false)
                    .build();
            voteList.clear();
            startControllerElection.set(false);
            startControllerElection();
        }
    }

    /**
     * 初始化Controller节点
     *
     * @param controllerId Controller节点
     */
    private void initController(Integer controllerId) throws Exception {
        controller = getController(controllerId);
        if (isForeController.get()) {
            // 我是新加入的小弟，发个消息让Controller重新分配一下
            RebalanceSlotsRequest rebalanceSlotsRequest = RebalanceSlotsRequest.newBuilder()
                    .setNameNodeId(nameNodeConfig.getNameNodeId())
                    .build();
            NettyPacket nettyPacket = NettyPacket.buildPacket(rebalanceSlotsRequest.toByteArray(), PacketType.RE_BALANCE_SLOTS);
            controller.rebalanceSlots(nettyPacket);
        }
        Map<Integer, Integer> slotMap = controller.initSlotAllocate();
        for (OnSlotAllocateCompletedListener listener : slotAllocateCompletedListeners) {
            listener.onSlotAllocateCompleted(slotMap);
            controller.addOnSlotAllocateCompletedListener(listener);
        }
    }

    private Controller getController(Integer controllerId) {
        if (nameNodeConfig.getNameNodeId() == controllerId) {
            // 自己是Controller
            controller = new LocalController(nameNodeConfig.getNameNodeId(), peerNameNodes,
                    diskNameSystem, dataNodeManager, userManager);
        } else {
            controller = new RemoteController(nameNodeConfig.getNameNodeId(), controllerId,
                    peerNameNodes, diskNameSystem, dataNodeManager);
        }
        synchronized (this) {
            notifyAll();
        }
        return controller;
    }

    /**
     * 从选票里获取最大的那个controller节点id
     */
    private Integer getBetterControllerNodeId(List<ControllerVote> votes) {
        int betterControllerNodeId = 0;
        for (ControllerVote vote : votes) {
            int controllerNodeId = vote.getControllerNodeId();
            if (controllerNodeId > betterControllerNodeId) {
                betterControllerNodeId = controllerNodeId;
            }
        }
        return betterControllerNodeId;
    }

    /**
     * 从选票中归票出来Controller节点
     *
     * @param votes  选票
     * @param quorum quorum
     * @return Controller节点ID
     */
    private Integer getControllerFromVotes(List<ControllerVote> votes, int quorum) {
        synchronized (this) {
            Map<Integer, Integer> voteCountMap = new HashMap<>(votes.size());
            for (ControllerVote vote : votes) {
                if (vote.getForce()) {
                    // 如果票据是强制有效的，直接认定是Controller节点
                    isForeController.set(true);
                    voteRound.set(vote.getVoteRound());
                    this.currentVote = ControllerVote.newBuilder()
                            .setVoterNodeId(nameNodeConfig.getNameNodeId())
                            .setControllerNodeId(vote.getControllerNodeId())
                            .setVoteRound(voteRound.get())
                            .setForce(true)
                            .build();
                    log.info("收到强制性选票，更新Controller信息：[currentNodeId={}, controllerId={}, voteRound={}]",
                            nameNodeConfig.getNameNodeId(), vote.getControllerNodeId(), voteRound.get());
                    return vote.getControllerNodeId();
                }
                Integer controllerNodeId = vote.getControllerNodeId();
                Integer count = voteCountMap.get(controllerNodeId);
                if (count == null) {
                    count = 0;
                }
                voteCountMap.put(controllerNodeId, ++count);
            }
            for (Map.Entry<Integer, Integer> voteCountEntry : voteCountMap.entrySet()) {
                if (voteCountEntry.getValue() >= quorum) {
                    return voteCountEntry.getKey();
                }
            }
        }
        return null;
    }

    /**
     * 收到Slot响应
     */
    public void onReceiveSlots(RequestWrapper requestWrapper) throws Exception {
        NameNodeSlots slots = NameNodeSlots.parseFrom(requestWrapper.getRequest().getBody());
        ensureControllerExists();
        controller.onReceiveSlots(slots);
    }

    private void ensureControllerExists() throws InterruptedException {
        while (controller == null) {
            synchronized (this) {
                wait(10);
            }
        }
    }

    /**
     * 收到重新分配Slots的请求
     *
     * @param requestWrapper 网络包
     */
    public void onRebalanceSlots(RequestWrapper requestWrapper) throws Exception {
        if (controller == null) {
            log.info("没有找到Controller");
            return;
        }
        if (controller instanceof RemoteController) {
            log.info("我不是Controller, 要重新分配Slots别找我");
            return;
        }
        controller.rebalanceSlots(requestWrapper.getRequest());
    }

    /**
     * 收到其他节点的元数据信息
     *
     * @param requestWrapper 其他节点
     * @throws Exception 异常
     */
    public void onFetchMetadata(RequestWrapper requestWrapper) throws Exception {
        controller.onFetchMetadata(requestWrapper);
    }


    /**
     * 将元数据发送给其他NameNode
     *
     * @throws Exception 异常
     */
    public void writeMetadataToPeer(RequestWrapper requestWrapper) throws Exception {
        FetchMetaDataRequest fetchMetaDataRequest = FetchMetaDataRequest.parseFrom(requestWrapper.getRequest().getBody());
        List<Integer> slotsList = fetchMetaDataRequest.getSlotsList();
        FetchMetaDataResponse.Builder builder = FetchMetaDataResponse.newBuilder()
                .setNodeId(nameNodeConfig.getNameNodeId());
        for (Integer slot : slotsList) {
            Set<Metadata> filesBySlot = diskNameSystem.getFilesBySlot(slot);
            for (Metadata metadata : filesBySlot) {
                FileInfo fileStorage = dataNodeManager.getFileStorage(metadata.getFileName());
                if (fileStorage != null) {
                    metadata = Metadata.newBuilder(metadata)
                            .setHostname(fileStorage.getHostname())
                            .setFileSize(fileStorage.getFileSize())
                            .build();
                    builder.addFiles(metadata);
                }
                if (builder.getFilesCount() >= 500) {
                    builder.setCompleted(false);
                    NettyPacket response = NettyPacket.buildPacket(builder.build().toByteArray(),
                            PacketType.FETCH_SLOT_METADATA_RESPONSE);
                    requestWrapper.sendResponse(response, null);
                    log.info("往别的NameNode发送不属于自己Slot的元数据：[targetNodeId={}, size={}]",
                            fetchMetaDataRequest.getNodeId(), builder.getFilesCount());
                    builder = FetchMetaDataResponse.newBuilder().setNodeId(nameNodeConfig.getNameNodeId());
                }
            }
        }
        builder.setCompleted(true);
        NettyPacket response = NettyPacket.buildPacket(builder.build().toByteArray(),
                PacketType.FETCH_SLOT_METADATA_RESPONSE);
        requestWrapper.sendResponse(response, null);
        log.info("往别的NameNode发送不属于自己Slot的元数据：[size={}]", builder.getFilesCount());
    }

    /**
     * Controller收到申请重平衡的节点已经完成重平衡的通知，
     *
     * @param requestWrapper 请求发起的节点
     */
    public void onLocalControllerFetchSlotMetadataCompleted(RequestWrapper requestWrapper) throws Exception {
        RebalanceFetchMetadataCompletedEvent rebalanceFetchMetadataCompletedEvent =
                RebalanceFetchMetadataCompletedEvent.parseFrom(requestWrapper.getRequest().getBody());
        controller.onFetchSlotMetadataCompleted(rebalanceFetchMetadataCompletedEvent.getRebalanceNodeId());
    }

    /**
     * 普通节点收到Controller节点的完成拉取元数据广播
     *
     * @param requestWrapper 网络包
     */
    public void onRemoteControllerFetchSlotMetadataCompleted(RequestWrapper requestWrapper) throws Exception {
        RebalanceFetchMetadataCompletedEvent event = RebalanceFetchMetadataCompletedEvent.parseFrom(requestWrapper.getRequest().getBody());
        controller.onFetchSlotMetadataCompleted(event.getRebalanceNodeId());
    }


    /**
     * Controller节点收到其他所有的NameNode节点的删除元数据完成的请求
     *
     * @param requestWrapper 网络包
     */
    public void onRemoveMetadataCompleted(RequestWrapper requestWrapper) throws Exception {
        controller.onRemoveMetadataCompleted(requestWrapper.getRequest());
    }

    public Map<Integer, Integer> getSlotNodeMap() {
        if (controller == null) {
            return new HashMap<>();
        }
        return controller.getSlotNodeMap();
    }

    /**
     * @param slot 槽位
     */
    public int getNodeIdBySlot(int slot) {
        return controller.getNodeIdBySlot(slot);
    }

    public void addOnSlotAllocateCompletedListener(OnSlotAllocateCompletedListener listener) {
        this.slotAllocateCompletedListeners.add(listener);
    }

}
