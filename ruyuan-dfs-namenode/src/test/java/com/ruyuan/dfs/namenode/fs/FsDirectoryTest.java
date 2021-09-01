package com.ruyuan.dfs.namenode.fs;

import com.ruyuan.dfs.common.enums.NodeType;
import com.ruyuan.dfs.common.utils.PrettyCodes;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 文件目录树单元测试类
 *
 * @author Sun Dasheng
 */
public class FsDirectoryTest {

    @Test
    public void mkdir() {
        FsDirectory fsDirectory = new FsDirectory();
        String filename = "/user/root";

        fsDirectory.mkdir(filename, new HashMap<>(PrettyCodes.trimMapSize()));

        Node node = fsDirectory.listFiles("/user");

        assertNotNull(node);

        Node root = node.getChildren().get("root");
        assertNotNull(root);

        assertEquals(root.getType(), NodeType.DIRECTORY.getValue());
    }

    @Test
    public void createFile() {
        FsDirectory fsDirectory = new FsDirectory();
        String filename = "/user/root/a.png";

        boolean success = fsDirectory.createFile(filename, new HashMap<>(PrettyCodes.trimMapSize()));
        assertTrue(success);

        success = fsDirectory.createFile(filename, new HashMap<>(PrettyCodes.trimMapSize()));
        assertFalse(success);

        Node node = fsDirectory.listFiles("/user");
        assertNotNull(node);

        Node root = node.getChildren().get("root");
        assertNotNull(root);
        assertEquals(root.getType(), NodeType.DIRECTORY.getValue());

        Node aPic = root.getChildren().get("a.png");
        assertNotNull(aPic);
        assertEquals(aPic.getType(), NodeType.FILE.getValue());
    }

    @Test
    public void deleteFile() {
        FsDirectory fsDirectory = new FsDirectory();
        String filename = "/user/root/a.png";

        boolean success = fsDirectory.createFile(filename, new HashMap<>(PrettyCodes.trimMapSize()));
        assertTrue(success);

        Node node = fsDirectory.listFiles(filename);
        assertNotNull(node);
        assertEquals(node.getType(), NodeType.FILE.getValue());

        Node delRet = fsDirectory.delete("/user/");
        assertNull(delRet);

        node = fsDirectory.listFiles(filename);
        assertNull(node);

    }

    @Test
    public void testMultiWriteRead() throws InterruptedException {
        FsDirectory fsDirectory = new FsDirectory();
        int threadNum = 1000;
        Queue<String> queue = new ConcurrentLinkedQueue<>();
        for (int i = 1; i < 100000; i++) {
            int parent = i / 1024;
            int child = i % 1024;
            String parentPath = String.format("%03d", parent);
            String childPath = String.format("%03d", child);
            String path = File.separator + parentPath + File.separator + childPath + File.separator + i + ".jpg";
            queue.add(path);
        }
        CountDownLatch latch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            new Thread(() -> {
                String filename;
                while ((filename = queue.poll()) != null) {
                    fsDirectory.createFile(filename, new HashMap<>());
                    Node node = fsDirectory.listFiles(filename);
                    assertNotNull(node);
                }
                latch.countDown();
            }).start();
        }
        latch.await();
    }

    @Test
    public void testFindAllFiles() {
        FsDirectory fsDirectory = new FsDirectory();
        String file1 = "/aaa/bbb/c1.png";
        String file2 = "/aaa/bbb/c2.png";
        String file3 = "/bbb/bbb/c2.png";
        fsDirectory.createFile(file1, new HashMap<>());
        fsDirectory.createFile(file2, new HashMap<>());
        String basePath = "/aaa";
        List<String> allFiles = fsDirectory.findAllFiles(basePath);
        List<String> filenames = new ArrayList<>();
        for (String file : allFiles) {
            filenames.add(basePath + file);
        }
        assertTrue(filenames.contains(file1));
        assertTrue(filenames.contains(file2));
        assertFalse(filenames.contains(file3));
    }
}