package com.ruyuan.dfs.namenode.server.tomcat.annotation;


import java.lang.annotation.*;

/**
 * 路径参数
 *
 * @author Sun Dasheng
 */
@Documented
@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface PathVariable {

    /**
     * 参数值
     *
     * @return 参数值
     */
    String value() default "";

}
