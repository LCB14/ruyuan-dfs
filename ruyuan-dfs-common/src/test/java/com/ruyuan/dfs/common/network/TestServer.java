package com.ruyuan.dfs.common.network;

import com.ruyuan.dfs.common.NettyPacket;
import com.ruyuan.dfs.model.client.MkdirRequest;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Sun Dasheng
 */
@Slf4j
public class TestServer {

    private static class TestHandler extends AbstractChannelHandler {

        @Override
        protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket nettyPacket) {
            RequestWrapper requestWrapper = new RequestWrapper(ctx, nettyPacket);
            MkdirRequest.Builder builder = MkdirRequest.newBuilder();
            for (int i = 0; i < 10000; i++) {
                builder.putAttr(String.valueOf(i), String.valueOf(i));
            }
            MkdirRequest response = builder.build();
            requestWrapper.sendResponse(response);
            return true;
        }

        @Override
        protected Set<Integer> interestPackageTypes() {
            return new HashSet<>();
        }
    }


}
