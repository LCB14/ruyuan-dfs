package com.ruyuan.dfs.namenode.server.tomcat.controller;

import com.ruyuan.dfs.namenode.server.tomcat.annotation.RequestMapping;
import com.ruyuan.dfs.namenode.server.tomcat.annotation.RestController;
import com.ruyuan.dfs.namenode.server.tomcat.domain.CommonResponse;
import com.ruyuan.dfs.namenode.server.tomcat.domain.LoginVO;
import lombok.extern.slf4j.Slf4j;

/**
 * 系统管理Controller
 *
 * @author Sun Dasheng
 */
@Slf4j
@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private static final String ADMIN = "admin";

    /**
     * 登陆
     */
    @RequestMapping("/login")
    public CommonResponse<Boolean> login(LoginVO loginVO) {
        if (ADMIN.equalsIgnoreCase(loginVO.getUsername()) && ADMIN.equalsIgnoreCase(loginVO.getPassword())) {
            return CommonResponse.successWith(true);
        }
        return CommonResponse.failWith("密码错误");
    }

}
