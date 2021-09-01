package com.ruyuan.dfs.namenode.server.tomcat;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Sun Dasheng
 */
public class VariablePathParserTest {

    @Test
    public void match() {
        VariablePathParser parser = new VariablePathParser();
        String schema1 = "/api/user/{user}/bbb/{userName}";
        String schema2 = "/api/user/{userId}";
        parser.add(schema1);
        parser.add(schema2);
        String path1 = "/api/user/wang/bbb/28";
        String path2 = "/api/user/123";
        String path3 = "/api/user";

        assertEquals(parser.match(path1), schema1);
        assertEquals(parser.match(path2), schema2);

        assertNull(parser.match(path3));

        assertEquals(parser.extractVariable(path1).get("user"), "wang");
        assertEquals(parser.extractVariable(path1).get("userName"), "28");
        assertEquals(parser.extractVariable(path2).get("userId"), "123");
    }
}