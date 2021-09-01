package com.ruyuan.dfs.namenode.server.handler;

import com.ruyuan.dfs.common.utils.DefaultScheduler;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.fs.DiskNameSystem;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

/**
 * @author Sun Dasheng
 */
public class RemoveFileHandlerTest {

    @Test
    public void handlerReq() throws Exception {
        NameNodeConfig nameNodeConfig = new NameNodeConfig();
        nameNodeConfig.setNameNodeLaunchMode("single");
        nameNodeConfig.setEditLogFlushThreshold(1000000);
        nameNodeConfig.setBaseDir("/tmp/ruyuan-dfs");
        DiskNameSystem diskNameSystem = new DiskNameSystem(nameNodeConfig,
                new DefaultScheduler("test"), null, null);

        diskNameSystem.createFile("/admin/aaa/bbb/a.png", new HashMap<>());
        diskNameSystem.createFile("/admin/aaa/bbb/b.png", new HashMap<>());
        diskNameSystem.createFile("/admin/aaa/bbb/c.png", new HashMap<>());
        diskNameSystem.mkdir("/admin/aaa/ccc", new HashMap<>());
    }
}