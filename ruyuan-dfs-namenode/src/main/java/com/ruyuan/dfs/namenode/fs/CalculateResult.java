package com.ruyuan.dfs.namenode.fs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 计算结果
 *
 * @author Sun Dasheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CalculateResult {

    private int fileCount;
    private long totalSize;

    public void addFileCount() {
        this.fileCount++;
    }

    public void addTotalSize(long size) {
        this.totalSize += size;
    }

    public void addFileCount(int fileCount) {
        this.fileCount += fileCount;
    }
}
