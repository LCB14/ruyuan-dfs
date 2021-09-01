package com.ruyuan.dfs.namenode.server.tomcat.domain;

import lombok.Data;

/**
 * 基本返回结构
 *
 * @author Sun Dasheng
 */
@Data
public class CommonResponse<T> {
    public static final Integer CODE_SUCCESS = 0;
    public static final Integer CODE_FAIL = -1;
    private int code;
    private String message;
    private String loggerId;
    private T data;
    public static <T> CommonResponse<T> successWith(T data) {
        CommonResponse<T> r = new CommonResponse<>();
        r.setCode(CODE_SUCCESS);
        r.setMessage("成功");
        r.setData(data);
        return r;
    }

    public static <T> CommonResponse<T> failWith(String message) {
        return failWith(CODE_FAIL, message, null);
    }

    public static <T> CommonResponse<T> failWith(Integer code, String message) {
        return failWith(code, message, null);
    }


    public static <T> CommonResponse<T> failWith(Integer code, String message, T data) {
        CommonResponse<T> r = new CommonResponse<>();
        r.setCode(code);
        r.setMessage(message);
        r.setData(data);
        return r;
    }

}
