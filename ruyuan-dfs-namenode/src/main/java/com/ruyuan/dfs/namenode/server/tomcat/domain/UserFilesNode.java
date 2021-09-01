package com.ruyuan.dfs.namenode.server.tomcat.domain;

import com.ruyuan.dfs.common.Constants;
import com.ruyuan.dfs.common.utils.FileUtil;
import com.ruyuan.dfs.namenode.fs.Node;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 用户文件节点
 *
 * @author Sun Dasheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserFilesNode {

    private String path;
    private int type;
    private List<UserFilesNode> children;
    private Map<String, String> attr;
    private String fileSize;

    public static UserFilesNode toUserFileNode(Node node, String path) {
        if (node == null) {
            return null;
        }
        if (!path.contains(Constants.TRASH_DIR) && node.getPath().equals(Constants.TRASH_DIR)) {
            return null;
        }
        UserFilesNode userFilesNode = new UserFilesNode();
        userFilesNode.setPath(node.getPath());
        userFilesNode.setAttr(node.getAttr());
        userFilesNode.setType(node.getType());
        long fileSize = Long.parseLong(node.getAttr().getOrDefault(Constants.ATTR_FILE_SIZE, "0"));
        userFilesNode.setFileSize(FileUtil.formatSize(fileSize));
        List<UserFilesNode> children = new LinkedList<>();
        for (String key : node.getChildren().keySet()) {
            Node child = node.getChildren().get(key);
            UserFilesNode childNode = toUserFileNode(child, path);
            if (childNode == null) {
                continue;
            }
            children.add(childNode);
        }
        userFilesNode.setChildren(children);
        return userFilesNode;
    }
}
