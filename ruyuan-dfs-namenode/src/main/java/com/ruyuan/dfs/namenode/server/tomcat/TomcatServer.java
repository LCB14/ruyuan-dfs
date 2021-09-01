package com.ruyuan.dfs.namenode.server.tomcat;

import com.ruyuan.dfs.common.metrics.MetricsServlet;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.datanode.DataNodeManager;
import com.ruyuan.dfs.namenode.fs.DiskNameSystem;
import com.ruyuan.dfs.namenode.server.NameNodeApis;
import com.ruyuan.dfs.namenode.server.UserManager;
import com.ruyuan.dfs.namenode.server.tomcat.servlet.CorsFilter;
import com.ruyuan.dfs.namenode.server.tomcat.servlet.DispatchComponentProvider;
import com.ruyuan.dfs.namenode.server.tomcat.servlet.DispatcherServlet;
import com.ruyuan.dfs.namenode.server.tomcat.servlet.FileDownloadServlet;
import com.ruyuan.dfs.namenode.shard.ShardingManager;
import com.ruyuan.dfs.namenode.shard.peer.PeerNameNodes;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.Context;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.descriptor.web.FilterDef;
import org.apache.tomcat.util.descriptor.web.FilterMap;

import java.nio.charset.StandardCharsets;

/**
 * 内置的Tomcat容器
 *
 * @author Sun Dasheng
 */
@Slf4j
public class TomcatServer {

    private int port;
    private Tomcat tomcat;
    private NameNodeConfig nameNodeConfig;
    private ShardingManager shardingManager;
    private PeerNameNodes peerNameNodes;
    private DataNodeManager dataNodeManager;
    private DispatcherServlet dispatcherServlet;
    private FileDownloadServlet fileDownloadServlet;

    public TomcatServer(NameNodeConfig nameNodeConfig, DataNodeManager dataNodeManager, PeerNameNodes peerNameNodes,
                        ShardingManager shardingManager, UserManager userManager, DiskNameSystem nameSystem,
                        NameNodeApis nameNodeApis) {
        DispatchComponentProvider.getInstance().addComponent(nameNodeConfig, dataNodeManager, peerNameNodes,
                shardingManager, userManager, nameSystem, nameNodeApis);
        this.tomcat = new Tomcat();
        this.nameNodeConfig = nameNodeConfig;
        this.dataNodeManager = dataNodeManager;
        this.peerNameNodes = peerNameNodes;
        this.shardingManager = shardingManager;
        this.port = nameNodeConfig.getHttpPort();
        this.dispatcherServlet = new DispatcherServlet();
        this.fileDownloadServlet = new FileDownloadServlet(nameNodeConfig, dataNodeManager, peerNameNodes, shardingManager);
    }


    public void start() {
        tomcat.setHostname("localhost");
        tomcat.setPort(port);
        Context context = tomcat.addContext("", null);
        Tomcat.addServlet(context, FileDownloadServlet.class.getSimpleName(), fileDownloadServlet);
        Tomcat.addServlet(context, DispatcherServlet.class.getSimpleName(), dispatcherServlet);
        context.addServletMappingDecoded("/api/*", DispatcherServlet.class.getSimpleName());
        context.addServletMappingDecoded("/*", FileDownloadServlet.class.getSimpleName());
        Tomcat.addServlet(context, MetricsServlet.class.getSimpleName(), new MetricsServlet());
        context.addServletMappingDecoded("/metrics", MetricsServlet.class.getSimpleName());

        FilterDef filterDef = new FilterDef();
        filterDef.setFilter(new CorsFilter());
        filterDef.setFilterName("CorsFilter");
        FilterMap filterMap = new FilterMap();
        filterMap.addURLPatternDecoded("/*");
        filterMap.addServletName("*");
        filterMap.setFilterName("CorsFilter");
        filterMap.setCharset(StandardCharsets.UTF_8);
        context.addFilterDef(filterDef);
        context.addFilterMap(filterMap);
        try {
            tomcat.init();
            tomcat.start();
            log.info("Tomcat启动成功：[port={}]", port);
        } catch (Exception e) {
            log.error("Tomcat启动失败：", e);
            System.exit(0);
        }
    }

    public void shutdown() {
        try {
            tomcat.stop();
        } catch (Exception e) {
            log.error("Tomcat停止失败：", e);
        }
    }
}
