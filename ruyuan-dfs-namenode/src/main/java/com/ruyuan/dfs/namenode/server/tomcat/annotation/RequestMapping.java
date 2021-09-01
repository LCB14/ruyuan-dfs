package com.ruyuan.dfs.namenode.server.tomcat.annotation;

import java.lang.annotation.*;

/**
 * 请求映射关系
 *
 * @author Sun Dasheng
 */
@Documented
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RequestMapping {

    /**
     * 请求路径
     *
     * @return 请求路径
     */
    String value() default "";

    /**
     * 请求方式
     *
     * @return 请求方式
     */
    String method() default "GET";
}
