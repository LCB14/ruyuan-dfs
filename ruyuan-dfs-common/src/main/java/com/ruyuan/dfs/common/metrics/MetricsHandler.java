package com.ruyuan.dfs.common.metrics;

import com.alibaba.fastjson.JSONObject;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * 监控Handler,集成在Netty的Http服务器中
 *
 * @author Sun Dasheng
 */
@Slf4j
public class MetricsHandler {

    private CollectorRegistry registry;

    public MetricsHandler() {
        this.registry = CollectorRegistry.defaultRegistry;
    }

    public boolean sendMetrics(ChannelHandlerContext ctx, FullHttpRequest request) throws IOException {
        String contentType = TextFormat.chooseContentType(request.headers().get("Accept"));
        String uri = request.uri();
        QueryStringDecoder decoder = new QueryStringDecoder(uri);
        List<String> isCloudPlatform = decoder.parameters().get("isCloudPlatform");
        if (!"/metrics".equals(decoder.path())) {
            return false;
        }
        if (isCloudPlatform != null && isCloudPlatform.size() > 0) {
            String isCloudPlatformValue = isCloudPlatform.get(0);
            if (isCloudPlatformValue.equals("true")) {
                FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK);
                response.headers().set("Content-Type", "application/json; charset=utf8");
                Enumeration<Collector.MetricFamilySamples> enumeration = registry.metricFamilySamples();
                List<Collector.MetricFamilySamples> ret = new ArrayList<>();
                while (enumeration.hasMoreElements()) {
                    Collector.MetricFamilySamples samples = enumeration.nextElement();
                    ret.add(samples);
                }
                String result = JSONObject.toJSONString(ret);
                response.content().writeBytes(Unpooled.copiedBuffer(result.getBytes()));
                ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                return true;
            }
        }
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK);
        response.headers().set("Content-Type", contentType);
        try (StringWriter writer = new StringWriter()) {
            TextFormat.writeFormat(contentType, writer, registry.filteredMetricFamilySamples(parse(request)));
            writer.flush();
            response.content().writeBytes(writer.toString().getBytes());
        }
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        return true;
    }

    private Set<String> parse(FullHttpRequest req) {
        QueryStringDecoder decoder = new QueryStringDecoder(req.uri());
        List<String> list = decoder.parameters().get("name");
        if (list == null) {
            return Collections.emptySet();
        } else {
            return new HashSet<>(list);
        }
    }
}
