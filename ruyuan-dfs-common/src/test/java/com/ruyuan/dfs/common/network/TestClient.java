package com.ruyuan.dfs.common.network;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.common.enums.PacketType;
import com.ruyuan.dfs.common.exception.RequestTimeoutException;
import com.ruyuan.dfs.common.utils.DefaultScheduler;
import com.ruyuan.dfs.model.client.MkdirRequest;

/**
 * @author Sun Dasheng
 */
public class TestClient {

    public static void main(String[] args) throws RequestTimeoutException, InterruptedException, InvalidProtocolBufferException {
        NetClient netClient = new NetClient("testClient", new DefaultScheduler("testClient"));
        netClient.connect("localhost", 8000);
        netClient.ensureConnected();
        NettyPacket nettyPacket = NettyPacket.buildPacket(new byte[0], PacketType.UNKNOWN);
        nettyPacket.setSupportChunked(false);
        NettyPacket nettyPacket1 = netClient.sendSync(nettyPacket);
        MkdirRequest mkdirRequest = MkdirRequest.parseFrom(nettyPacket1.getBody());
        System.out.println(mkdirRequest.getAttrMap());
    }
}
