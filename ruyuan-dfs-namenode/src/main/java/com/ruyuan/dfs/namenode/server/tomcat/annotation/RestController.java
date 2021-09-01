package com.ruyuan.dfs.namenode.server.tomcat.annotation;

import java.lang.annotation.*;

/**
 * Rest风格的Controller
 *
 * @author Sun Dasheng
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RestController {

    String value() default "";

}
