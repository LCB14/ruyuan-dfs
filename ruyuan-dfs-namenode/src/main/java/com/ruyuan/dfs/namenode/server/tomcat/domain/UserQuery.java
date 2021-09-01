package com.ruyuan.dfs.namenode.server.tomcat.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 用户查询条件
 *
 * @author Sun Dasheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserQuery {

    private String name;
    private String age;
}
