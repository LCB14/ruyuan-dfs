package com.ruyuan.dfs.common.ha;

import com.google.common.collect.Sets;
import com.ruyuan.dfs.common.Constants;
import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.enums.PacketType;
import com.ruyuan.dfs.common.network.AbstractChannelHandler;
import com.ruyuan.dfs.common.utils.ByteUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * BackupNode查询NameNode的状态请求
 *
 * @author Sun Dasheng
 */
@Slf4j
public class ReportNameNodeStatusHandler extends AbstractChannelHandler {

    private AtomicBoolean nameNodeDown = new AtomicBoolean(false);

    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket nettyPacket) {
        // 获取NameNode状态的请求
        byte[] status = new byte[4];
        ByteUtil.setInt(status, 0, nameNodeDown.get() ? Constants.NAMENODE_STATUS_DOWN : Constants.NAMENODE_STATUS_UP);
        NettyPacket req = NettyPacket.buildPacket(status, PacketType.GET_NAME_NODE_STATUS);
        ctx.writeAndFlush(req);
        if (log.isDebugEnabled()) {
            log.debug("BackupNode获取NameNode的状态：[nameNodeDown={}]", nameNodeDown.get());
        }
        return true;
    }

    @Override
    protected Set<Integer> interestPackageTypes() {
        return Sets.newHashSet(PacketType.GET_NAME_NODE_STATUS.getValue());
    }

    /**
     * 标识NameNode节点宕机了
     */
    public void markNameNodeDown() {
        nameNodeDown.compareAndSet(false, true);
    }


}
