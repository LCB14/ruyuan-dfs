package com.ruyuan.dfs.namenode.server.tomcat.annotation;

import java.lang.annotation.*;

/**
 * 请求参数JSON格式
 *
 * @author Sun Dasheng
 */
@Documented
@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface RequestBody {
}
