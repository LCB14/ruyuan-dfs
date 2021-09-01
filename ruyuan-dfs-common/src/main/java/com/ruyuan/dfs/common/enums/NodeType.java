package com.ruyuan.dfs.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 文件节点类型
 *
 * @author Sun Dasheng
 */
@Getter
@AllArgsConstructor
public enum NodeType {
    /**
     * 文件节点类型
     */
    FILE(1), DIRECTORY(2);

    private int value;

}