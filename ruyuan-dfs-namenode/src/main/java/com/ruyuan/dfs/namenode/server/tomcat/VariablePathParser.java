package com.ruyuan.dfs.namenode.server.tomcat;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * 路径变量解析器
 *
 * @author Sun Dasheng
 */
@Slf4j
public class VariablePathParser {

    private static final Pattern PATH_VARIABLE_PATTERN = Pattern.compile("\\{([^}])*}");

    private Node root;
    private int size;

    public VariablePathParser() {
        this.root = new Node();
        this.size = 0;
    }

    public int getSize() {
        return size;
    }

    /**
     * 添加一个请求映射路径
     *
     * @param uri 路径 eg: /api/{user}/{age} 这样的路径
     */
    public void add(String uri) {
        String[] paths = uri.split("/");
        Node cur = root;
        for (String path : paths) {
            if (path == null || path.length() == 0) {
                continue;
            }
            if (cur.next.get(path) == null) {
                Node node = new Node();
                node.path = path;
                node.isVariable = containVariable(path);
                cur.next.put(path, node);
            }
            cur = cur.next.get(path);
        }
        cur.isEnd = true;
        size++;
    }

    /**
     * 判断映射列表中是否能找到和给定路径匹配的映射关系
     * <p>
     * 比如，映射关系保存了: /api/{user}/{age}这样的映射
     * <p>
     * 那么传入 /api/aaa/222 这样的路径就能关联得上，并返回 /api/{user}/{age}
     *
     * @param uri 路径
     * @return 映射关系列表
     */
    public String match(String uri) {
        String[] paths = uri.substring(1).split("/");
        String[] ret = new String[paths.length];
        Node cur = root;
        Node node = find(paths, cur, 0, ret);
        if (node != null && node.isEnd) {
            return "/" + String.join("/", ret);
        }
        return null;
    }

    private Node find(String[] paths, Node cur, int index, String[] ret) {
        if (index == paths.length) {
            return cur.isEnd ? cur : null;
        }
        String path = paths[index];
        ret[index] = path;
        if (cur.next.get(path) != null) {
            ret[index] = path;
            cur = cur.next.get(path);
            return find(paths, cur, index + 1, ret);
        } else {
            Node node = null;
            for (String key : cur.next.keySet()) {
                Node n = cur.next.get(key);
                if (n.isVariable) {
                    node = find(paths, n, index + 1, ret);
                }
                if (node != null) {
                    ret[index] = key;
                    break;
                }
            }
            return node;
        }
    }


    /**
     * 判断请求路径是否包含变量
     *
     * @param uri 请求路径
     * @return 是否包含变量
     */
    public boolean containVariable(String uri) {
        return PATH_VARIABLE_PATTERN.matcher(uri).find();
    }

    /**
     * <pre>
     *
     * 根据给定的url和设置的匹配路径，进行匹配，获取到变量
     *
     * 假设设置了一个路径： /api/user/{uerId}
     *
     * 此时传入： /api/user/123
     *
     * 则会返回一个map：
     *  {
     *      "userId": 123
     *  }
     * </pre>
     *
     * @param uri 请求URI
     * @return 匹配出来的变量参数
     */
    public Map<String, String> extractVariable(String uri) {
        String[] paths = uri.substring(1).split("/");
        return match(root, paths, 0);
    }

    private Map<String, String> match(Node node, String[] paths, int index) {
        Map<String, String> ret = new HashMap<>(2);
        if (index == paths.length) {
            return ret;
        }
        String path = paths[index];
        if (node.next.get(path) != null) {
            ret.putAll(match(node.next.get(path), paths, index + 1));
        } else {
            for (String key : node.next.keySet()) {
                Node n = node.next.get(key);
                if (n.isVariable) {
                    String varKey = key.replaceAll("\\{", "").replaceAll("}", "");
                    ret.put(varKey, path);
                    ret.putAll(match(n, paths, index + 1));
                }
            }
        }
        return ret;
    }

    private static class Node {
        boolean isVariable;
        TreeMap<String, Node> next;
        boolean isEnd;
        String path;

        public Node(boolean isVariable) {
            this.isVariable = isVariable;
            this.next = new TreeMap<>();
        }

        public Node() {
            this(false);
        }
    }
}
