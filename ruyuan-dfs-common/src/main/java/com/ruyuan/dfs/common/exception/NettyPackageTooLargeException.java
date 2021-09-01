package com.ruyuan.dfs.common.exception;

/**
 * 网络包太大的异常
 *
 * @author Sun Dasheng
 */
public class NettyPackageTooLargeException extends Exception {

    public NettyPackageTooLargeException() {
    }

    public NettyPackageTooLargeException(String message) {
        super(message);
    }
}
