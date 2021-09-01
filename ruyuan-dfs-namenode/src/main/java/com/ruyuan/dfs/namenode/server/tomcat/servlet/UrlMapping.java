package com.ruyuan.dfs.namenode.server.tomcat.servlet;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 请求路径映射
 *
 * @author Sun Dasheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UrlMapping {


    /**
     * 请求URL
     */
    private String url;
    /**
     * 请求方式
     */
    private String method;
    /**
     * 调用的方法
     */
    private Method invokeMethod;

    private List<ParamMetadata> parameterList = new LinkedList<>();

    public void addParameterList(Type type, String paramKey, Class<?> paramClassType) {
        parameterList.add(new ParamMetadata(type, paramKey, paramClassType));
    }

    @Data
    @AllArgsConstructor
    public class ParamMetadata {
        Type type;
        String paramKey;
        Class<?> paramClassType;
    }

    public enum Type {
        PATH_VARIABLE, REQUEST_BODY, QUERY_ENTITY
    }
}
