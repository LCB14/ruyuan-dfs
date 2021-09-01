package com.ruyuan.dfs.common.exception;

/**
 * 请求超时异常
 *
 * @author Sun Dasheng
 */
public class RequestTimeoutException extends Exception {
    public RequestTimeoutException(String s) {
        super(s);
    }
}
