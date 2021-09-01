package com.ruyuan.dfs.namenode.fs;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Sun Dasheng
 */
public class NodeTests {

    @Test
    public void testGetFullPath() {
        Node a = new Node("/", 1);
        Node b = new Node("bbb", 1);
        Node c = new Node("ccc", 1);
        a.addChildren(b);
        b.addChildren(c);
        assertEquals("/bbb/ccc", c.getFullPath());
    }

}
