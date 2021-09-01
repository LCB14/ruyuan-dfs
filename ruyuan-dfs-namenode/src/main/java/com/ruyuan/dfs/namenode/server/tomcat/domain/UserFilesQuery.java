package com.ruyuan.dfs.namenode.server.tomcat.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 用户文件列表查询
 *
 * @author Sun Dasheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserFilesQuery {

    private String username;
    private String path;

}
