package com.ruyuan.dfs.namenode.server.tomcat.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 分页结果
 *
 * @author Sun Dasheng
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CommonPageResponse<T> {

    private int total;
    private int pageIndex;
    private int pageSize;
    private List<T> list;

    public static <T> CommonPageResponse<T> successWith(List<T> list, int pageIndex, int pageSize, int total) {
        CommonPageResponse<T> response = new CommonPageResponse<>();
        response.setPageIndex(pageIndex);
        response.setPageSize(pageSize);
        response.setTotal(total);
        response.setList(list);
        return response;
    }

}