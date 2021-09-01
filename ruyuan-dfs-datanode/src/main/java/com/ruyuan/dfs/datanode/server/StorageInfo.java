package com.ruyuan.dfs.datanode.server;

import com.ruyuan.dfs.common.FileInfo;
import lombok.Data;

import java.util.List;

/**
 * 存储信息
 *
 * @author Sun Dasheng
 */
@Data
public class StorageInfo {

    /**
     * 文件信息
     */
    private List<FileInfo> files;
    /**
     * 已用空间
     */
    private long storageSize;
    /**
     * 可用空间
     */
    private long freeSpace;

    public StorageInfo() {
        this.storageSize = 0L;
    }
}
