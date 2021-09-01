package com.ruyuan.dfs.namenode.server.tomcat.annotation;

import java.lang.annotation.*;

/**
 * 自动注入
 *
 * @author Sun Dasheng
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Autowired {

    String value() default "";

}
