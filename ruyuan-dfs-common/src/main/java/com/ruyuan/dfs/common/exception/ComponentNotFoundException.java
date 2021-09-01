package com.ruyuan.dfs.common.exception;

/**
 * 找不到组件异常
 *
 * @author Sun Dasheng
 */
public class ComponentNotFoundException extends Exception {
    public ComponentNotFoundException() {
    }

    public ComponentNotFoundException(String message) {
        super(message);
    }
}
