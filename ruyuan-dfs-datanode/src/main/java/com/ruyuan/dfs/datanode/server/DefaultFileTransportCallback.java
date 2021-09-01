package com.ruyuan.dfs.datanode.server;

import com.ruyuan.dfs.common.metrics.Prometheus;
import com.ruyuan.dfs.common.network.file.FileAttribute;
import com.ruyuan.dfs.common.network.file.FileTransportCallback;
import com.ruyuan.dfs.datanode.namenode.NameNodeClient;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * 默认的文件传输回调
 *
 * @author Sun Dasheng
 */
@Slf4j
public class DefaultFileTransportCallback implements FileTransportCallback {

    private NameNodeClient nameNodeClient;
    private StorageManager storageManager;

    public DefaultFileTransportCallback(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    public void setNameNodeClient(NameNodeClient nameNodeClient) {
        this.nameNodeClient = nameNodeClient;
    }

    @Override
    public String getPath(String filename) {
        String localFileName = storageManager.getAbsolutePathByFileName(filename);
        log.info("获取文件路径文件：[filename={}, location={}]", filename, localFileName);
        return localFileName;
    }

    @Override
    public void onCompleted(FileAttribute fileAttribute) throws InterruptedException, IOException {
        storageManager.recordReplicaReceive(fileAttribute.getFilename(), fileAttribute.getAbsolutePath(), fileAttribute.getSize());
        nameNodeClient.informReplicaReceived(fileAttribute.getFilename(), fileAttribute.getSize());
    }

    @Override
    public void onProgress(String filename, long total, long current, float progress, int currentWriteBytes) {
        Prometheus.hit("datanode_disk_write_bytes", "DataNode瞬时写磁盘大小", currentWriteBytes);
    }
}
