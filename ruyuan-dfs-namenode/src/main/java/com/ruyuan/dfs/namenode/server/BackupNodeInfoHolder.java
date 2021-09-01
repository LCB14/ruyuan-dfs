package com.ruyuan.dfs.namenode.server;

import com.ruyuan.dfs.common.utils.NetUtils;
import com.ruyuan.dfs.model.backup.BackupNodeInfo;
import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 用于处理BackupNode的信息同步到客户端和DataNode的处理器
 *
 * @author Sun Dasheng
 */
@Getter
@AllArgsConstructor
public class BackupNodeInfoHolder {
    private BackupNodeInfo backupNodeInfo;
    private Channel channel;


    public boolean isActive() {
        return channel.isActive();
    }

    public boolean match(Channel channel) {
        return NetUtils.getChannelId(channel).equals(NetUtils.getChannelId(this.channel));
    }
}