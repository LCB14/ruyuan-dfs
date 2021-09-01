package com.ruyuan.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.enums.UserChangeEventEnum;
import com.ruyuan.dfs.common.utils.*;
import com.ruyuan.dfs.model.namenode.UserChangeEvent;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.server.tomcat.domain.User;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 认证管理器
 *
 * @author Sun Dasheng
 */
@Slf4j
public class UserManager {

    private NameNodeConfig nameNodeConfig;
    private DefaultScheduler defaultScheduler;
    private Map<String, User> userMap;
    private Map<String, String> channelUserMap = new ConcurrentHashMap<>();
    private Map<String, Set<String>> userTokenMaps = new ConcurrentHashMap<>();

    public UserManager(NameNodeConfig nameNodeConfig, DefaultScheduler defaultScheduler) {
        this.defaultScheduler = defaultScheduler;
        this.nameNodeConfig = nameNodeConfig;
        this.userMap = loadUserInfoFromDisk(nameNodeConfig);
        this.defaultScheduler.schedule("刷新用户数据到磁盘", this::refreshAuthInfo,
                60 * 60 * 1000, 60 * 60 * 1000, TimeUnit.MILLISECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(this::refreshAuthInfo));
    }

    private Map<String, User> loadUserInfoFromDisk(NameNodeConfig nameNodeConfig) {
        String authInfoFile = nameNodeConfig.getAuthInfoFile();
        File file = new File(authInfoFile);
        if (file.exists()) {
            try {
                String s = FileUtil.readString(authInfoFile);
                List<User> users = JSONObject.parseArray(s, User.class);
                return users.stream().collect(Collectors.toMap(User::getUsername, Function.identity()));
            } catch (Exception e) {
                throw new RuntimeException("加载认证信息出错：", e);
            }
        }
        return new HashMap<>(PrettyCodes.trimMapSize());
    }

    /**
     * 认证，保存每个用户对应的
     *
     * @param authenticateInfo 认证信息
     */
    public boolean login(Channel channel, String authenticateInfo) {
        synchronized (this) {
            String[] split = authenticateInfo.split(",");
            String username = split[0];
            String secret = split[1];
            if (!userMap.containsKey(username)) {
                return false;
            }
            User user = userMap.get(username);
            if (!user.getSecret().equals(secret)) {
                return false;
            }
            String clientId = NetUtils.getChannelId(channel);
            Set<String> existsTokens = userTokenMaps.computeIfAbsent(username, k -> new HashSet<>());
            existsTokens.add(clientId + "-" + StringUtils.getRandomString(10));
            channelUserMap.put(clientId, user.getUsername());
            return true;
        }
    }


    /**
     * 登出
     *
     * @param channel 退出登陆
     * @return 退出登录的名字
     */
    public String logout(Channel channel) {
        synchronized (this) {
            String clientId = NetUtils.getChannelId(channel);
            String username = channelUserMap.remove(clientId);
            if (username != null) {
                Set<String> existsTokens = userTokenMaps.get(username);
                Iterator<String> iterator = existsTokens.iterator();
                while (iterator.hasNext()) {
                    String token = iterator.next();
                    String existsClientId = token.split("-")[0];
                    if (existsClientId.equals(clientId)) {
                        iterator.remove();
                    }
                }
                return username;
            }
            return null;
        }
    }

    /**
     * 获取用户对应的Token节点
     *
     * @param channel 客户端连接
     * @return Token信息
     */
    public String getTokenByChannel(Channel channel) {
        synchronized (this) {
            String clientId = NetUtils.getChannelId(channel);
            String username = channelUserMap.get(clientId);
            if (username == null) {
                return null;
            }
            Set<String> tokens = userTokenMaps.get(username);
            for (String token : tokens) {
                String existsClientId = token.split("-")[0];
                if (existsClientId.equals(clientId)) {
                    return token;
                }
            }
            return null;
        }
    }

    /**
     * 收到广播的认证请求保存token
     *
     * @param username  用户名
     * @param userToken token
     */
    public void setToken(String username, String userToken) {
        Set<String> existsTokens = userTokenMaps.computeIfAbsent(username, k -> new HashSet<>());
        existsTokens.add(userToken);
    }


    /**
     * 收到广播的用户退出登录请求
     *
     * @param userName 用户名
     * @param token    token
     */
    public void logout(String userName, String token) {
        Set<String> tokens = userTokenMaps.get(userName);
        tokens.remove(token);
    }

    /**
     * 获取用户对应的Token节点
     *
     * @param username 客户端连接
     * @return Token信息
     */
    public boolean isUserToken(String username, String token) {
        Set<String> tokens = userTokenMaps.getOrDefault(username, new HashSet<>());
        return tokens.contains(token);
    }

    /**
     * 新增一个用户
     *
     * @param user 新增一个用户
     */
    public void addOrUpdateUser(User user) {
        // TODO 高级功能：指定用户名为元数据hashKey，这样可以保证某个用户的所有元数据都在同一个节点上，
        //  同时在很多判断的地方改造，可以避免不必要的交互
        User containsUser = userMap.get(user.getUsername());
        if (containsUser != null) {
            containsUser.setSecret(user.getSecret());
            Set<String> existsDataNodes = containsUser.getStorageInfo().getDataNodesSet();
            for (String dataNode : user.getStorageInfo().getDataNodes()) {
                if (!existsDataNodes.contains(dataNode)) {
                    containsUser.getStorageInfo().addDataNode(dataNode);
                }
            }
        }
        userMap.put(user.getUsername(), user);
    }

    public boolean contain(String username) {
        return userMap.containsKey(username);
    }

    private void refreshAuthInfo() {
        try {
            String data = JSONObject.toJSONString(userMap.values());
            FileUtil.saveFile(nameNodeConfig.getAuthInfoFile(), true, ByteBuffer.wrap(data.getBytes()));
        } catch (Exception e) {
            throw new RuntimeException("刷新认证信息出错：", e);
        }
    }

    public List<User> getAllUser() {
        return userMap.values()
                .stream()
                .map(User::copy)
                .collect(Collectors.toList());
    }

    public User deleteUser(String username) {
        synchronized (this) {
            User user = userMap.remove(username);
            userTokenMaps.remove(username);
            return user;
        }
    }

    public void onUserChangeEvent(NettyPacket nettyPacket) throws InvalidProtocolBufferException {
        UserChangeEvent userChangeEvent = UserChangeEvent.parseFrom(nettyPacket.getBody());
        int eventType = userChangeEvent.getEventType();
        UserChangeEventEnum eventEnum = UserChangeEventEnum.getEnum(eventType);
        User user = User.parse(userChangeEvent.getUserEntity());
        log.debug("收到集群其他节点发送过来的更新用户信息：[event={}, user={}]", eventEnum, user);
        switch (eventEnum) {
            case ADD:
            case MODIFY:
                addOrUpdateUser(user);
                break;
            case DELETE:
                deleteUser(user.getUsername());
                break;
            default:
                break;
        }
    }

    /**
     * 更新当前用户的存储信息
     *
     * @param username 用户名
     * @param fileSize 文件内容大小
     */
    public void addStorageInfo(String username, long fileSize) {
        synchronized (this) {
            User user = userMap.get(username);
            if (user == null) {
                return;
            }
            User.StorageInfo storageInfo = user.getStorageInfo();
            storageInfo.setFileCount(storageInfo.getFileCount() + 1);
            storageInfo.setStorageSize(storageInfo.getStorageSize() + fileSize);
        }
    }

    /**
     * 更新当前用户的存储信息
     *
     * @param username 用户名
     * @param fileSize 文件内容大小
     */
    public void removeStorageInfo(String username, long fileSize) {
        synchronized (this) {
            User user = userMap.get(username);
            if (user == null) {
                return;
            }
            User.StorageInfo storageInfo = user.getStorageInfo();
            storageInfo.setFileCount(storageInfo.getFileCount() - 1);
            storageInfo.setStorageSize(storageInfo.getStorageSize() - fileSize);
        }
    }

    public User getUser(String username) {
        return User.copy(userMap.get(username));
    }
}
