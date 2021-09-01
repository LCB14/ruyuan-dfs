package com.ruyuan.dfs.namenode.fs;

import com.ruyuan.dfs.model.namenode.Metadata;

import java.util.Map;
import java.util.Set;

/**
 * 文件系统
 *
 * @author Sun Dasheng
 */
public interface FsNamesystem {

    /**
     * 创建文件夹
     *
     * @param path 文件路径
     * @param attr 文件属性
     */
    void mkdir(String path, Map<String, String> attr);

    /**
     * 创建文件
     *
     * @param filename 文件名称
     * @param attr 文件属性
     * @return 是否创建成功
     */
    boolean createFile(String filename, Map<String, String> attr);


    /**
     * 删除文件
     *
     * @param filename 文件名
     * @return 是否删除成功
     */
    boolean deleteFile(String filename);

    /**
     * 根据Slot获取文件名
     *
     * @param slot slot
     * @return 文件名
     */
    Set<Metadata> getFilesBySlot(int slot);

}
