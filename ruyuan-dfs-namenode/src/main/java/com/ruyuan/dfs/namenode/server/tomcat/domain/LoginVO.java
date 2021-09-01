package com.ruyuan.dfs.namenode.server.tomcat.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 登陆参数
 *
 * @author Sun Dasheng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LoginVO {

    private String username;
    private String password;
}
