package com.ruyuan.dfs.namenode.server.tomcat.controller;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.enums.NameNodeLaunchMode;
import com.ruyuan.dfs.common.enums.PacketType;
import com.ruyuan.dfs.common.enums.UserChangeEventEnum;
import com.ruyuan.dfs.common.exception.NameNodeException;
import com.ruyuan.dfs.model.namenode.*;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.datanode.DataNodeManager;
import com.ruyuan.dfs.namenode.fs.DiskNameSystem;
import com.ruyuan.dfs.namenode.fs.Node;
import com.ruyuan.dfs.namenode.server.NameNodeApis;
import com.ruyuan.dfs.namenode.server.UserManager;
import com.ruyuan.dfs.namenode.server.tomcat.annotation.*;
import com.ruyuan.dfs.namenode.server.tomcat.domain.*;
import com.ruyuan.dfs.namenode.shard.ShardingManager;
import com.ruyuan.dfs.namenode.shard.peer.PeerNameNodes;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 用户管理接口
 *
 * @author Sun Dasheng
 */
@Slf4j
@RestController
@RequestMapping("/api/user")
public class UserController {

    @Autowired
    private DataNodeManager dataNodeManager;

    @Autowired
    private UserManager userManager;

    @Autowired
    private PeerNameNodes peerNameNodes;

    @Autowired
    private DiskNameSystem diskNameSystem;

    @Autowired
    private NameNodeConfig nameNodeConfig;

    @Autowired
    private ShardingManager shardingManager;

    @Autowired
    private NameNodeApis nameNodeApis;

    @RequestMapping(value = "/", method = "POST")
    public CommonResponse<Boolean> addUser(@RequestBody User user) {
        if (user.getStorageInfo() == null) {
            user.setStorageInfo(new User.StorageInfo());
        }
        if (userManager.contain(user.getUsername())) {
            return CommonResponse.failWith("用户已存在");
        }
        userManager.addOrUpdateUser(user);
        broadcast(user, UserChangeEventEnum.ADD.getValue());
        return CommonResponse.successWith(true);
    }

    @RequestMapping(value = "/")
    public CommonResponse<List<User>> listUser() throws InvalidProtocolBufferException {
        List<User> users = userManager.getAllUser();
        maybeFetchFromOtherNode(users);
        users.sort((o1, o2) -> {
            if (o1.getCreateTime() == o2.getCreateTime()) {
                return 0;
            }
            return (int) (o1.getCreateTime() - o2.getCreateTime());

        });
        return CommonResponse.successWith(users);
    }

    @RequestMapping(value = "/{username}")
    public CommonResponse<User> getUserByUsername(@PathVariable("username") String username) throws InvalidProtocolBufferException {
        User user = userManager.getUser(username);
        maybeFetchFromOtherNode(Collections.singletonList(user));
        return CommonResponse.successWith(user);
    }

    /**
     * 修改用户，用户修改只支持 修改secret，和新增dataNode节点。不支持修改其他数据
     *
     * @param user 用户信息
     * @return 修改结果
     */
    @RequestMapping(value = "/", method = "PUT")
    public CommonResponse<Boolean> modifyUser(@RequestBody User user) {
        if (user.getStorageInfo() == null) {
            user.setStorageInfo(new User.StorageInfo());
        }
        userManager.addOrUpdateUser(user);
        broadcast(user, UserChangeEventEnum.MODIFY.getValue());
        return CommonResponse.successWith(true);
    }


    @RequestMapping(value = "/{username}", method = "DELETE")
    public CommonResponse<Boolean> deleteUser(@PathVariable("username") String username) {
        User user = userManager.deleteUser(username);
        if (user != null) {
            broadcast(user, UserChangeEventEnum.DELETE.getValue());
        }
        return CommonResponse.successWith(true);
    }

    @RequestMapping(value = "/listFiles")
    public CommonResponse<UserFilesNode> listFiles(UserFilesQuery userFilesQuery) throws InvalidProtocolBufferException {
        String basePath = File.separator + userFilesQuery.getUsername() + userFilesQuery.getPath();
        Node node = nameNodeApis.listNode(basePath);
        UserFilesNode userFilesNode = UserFilesNode.toUserFileNode(node, userFilesQuery.getPath());
        return CommonResponse.successWith(userFilesNode);
    }

    @RequestMapping(value = "/moveToTrash", method = "PUT")
    public CommonResponse<Integer> moveToTrash(@RequestBody UserFileMoveToTrashVO userFileMoveToTrashVO) throws NameNodeException,
            InvalidProtocolBufferException {
        List<String> paths = userFileMoveToTrashVO.getPaths();
        int count = nameNodeApis.removeFileOrDirInternal(paths, userFileMoveToTrashVO.getUsername());
        RemoveFileOrDirRequest request = RemoveFileOrDirRequest.newBuilder()
                .addAllFiles(paths)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.NAME_NODE_REMOVE_FILE);
        nettyPacket.setUsername(userFileMoveToTrashVO.getUsername());
        List<NettyPacket> responsePackages = peerNameNodes.broadcastSync(nettyPacket);
        for (NettyPacket responsePackage : responsePackages) {
            if (responsePackage.isSuccess()) {
                RemoveFileOrDirResponse response = RemoveFileOrDirResponse.parseFrom(responsePackage.getBody());
                count += response.getFileCount();
            }
        }
        return CommonResponse.successWith(count);
    }

    @RequestMapping(value = "/trash/resume", method = "PUT")
    public CommonResponse<Integer> resumeTrash(@RequestBody UserFileMoveToTrashVO userFileMoveToTrashVO) throws
            InvalidProtocolBufferException, NameNodeException {
        int count = nameNodeApis.trashResumeInternal(userFileMoveToTrashVO.getPaths(), userFileMoveToTrashVO.getUsername());
        TrashResumeRequest request = TrashResumeRequest.newBuilder()
                .addAllFiles(userFileMoveToTrashVO.getPaths())
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.TRASH_RESUME);
        nettyPacket.setUsername(userFileMoveToTrashVO.getUsername());
        List<NettyPacket> responsePackages = peerNameNodes.broadcastSync(nettyPacket);
        for (NettyPacket responsePackage : responsePackages) {
            if (responsePackage.isSuccess()) {
                TrashResumeResponse response = TrashResumeResponse.parseFrom(responsePackage.getBody());
                count += response.getFileCount();
            }
        }
        return CommonResponse.successWith(count);
    }

    private void broadcast(User user, int event) {
        UserChangeEvent userChangeEvent = UserChangeEvent.newBuilder()
                .setEventType(event)
                .setUserEntity(user.toEntity())
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(userChangeEvent.toByteArray(), PacketType.USER_CHANGE_EVENT);
        peerNameNodes.broadcast(nettyPacket);
    }


    private void maybeFetchFromOtherNode(List<User> users) throws InvalidProtocolBufferException {
        if (NameNodeLaunchMode.CLUSTER.equals(nameNodeConfig.getMode())) {
            // 因为各自节点都维护着某个用户的部分数据，所以需要去所有节点聚合所有的用户信息
            NettyPacket request = NettyPacket.buildPacket(new byte[0], PacketType.FETCH_USER_INFO);
            List<NettyPacket> responses = peerNameNodes.broadcastSync(request);
            for (NettyPacket response : responses) {
                UserList userList = UserList.parseFrom(response.getBody());
                Map<String, User> userMap = userList.getUserEntitiesList().stream()
                        .map(User::parse)
                        .collect(Collectors.toMap(User::getUsername, Function.identity()));
                merge(users, userMap);
            }
        }
    }

    private void merge(List<User> users, Map<String, User> userMap) {
        for (User user : users) {
            User userFromOtherNode = userMap.get(user.getUsername());
            if (userFromOtherNode == null) {
                continue;
            }
            User.StorageInfo otherStorageInfo = userFromOtherNode.getStorageInfo();
            if (otherStorageInfo == null) {
                continue;
            }
            User.StorageInfo storageInfo = user.getStorageInfo();
            storageInfo.setFileCount(storageInfo.getFileCount() + otherStorageInfo.getFileCount());
            storageInfo.setStorageSize(storageInfo.getStorageSize() + otherStorageInfo.getStorageSize());
            if (otherStorageInfo.getDataNodes() == null) {
                continue;
            }
            storageInfo.getDataNodes().addAll(otherStorageInfo.getDataNodes());
        }
    }
}
