package com.ruyuan.dfs.namenode.server.tomcat.servlet;

import java.util.HashMap;
import java.util.Map;

/**
 * 组件提供者
 *
 * @author Sun Dasheng
 */
public class DispatchComponentProvider {

    public static DispatchComponentProvider INSTANCE = null;

    public static DispatchComponentProvider getInstance() {
        if (INSTANCE == null) {
            synchronized (DispatchComponentProvider.class) {
                if (INSTANCE == null) {
                    INSTANCE = new DispatchComponentProvider();
                }
            }
        }
        return INSTANCE;
    }

    private Map<String, Object> components = new HashMap<>();

    public void addComponent(Object... objs) {
        for (Object obj : objs) {
            components.put(obj.getClass().getSimpleName(), obj);
        }
    }

    public Object getComponent(String key) {
        return components.get(key);
    }
}