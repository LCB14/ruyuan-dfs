package com.ruyuan.dfs.namenode.editslog;

import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

/**
 * 双缓冲机制
 *
 * @author Sun Dasheng
 */
@Slf4j
public class DoubleBuffer {

    private NameNodeConfig nameNodeConfig;
    private EditLogBuffer currentBuffer;
    private EditLogBuffer syncBuffer;

    public DoubleBuffer(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
        this.currentBuffer = new EditLogBuffer(nameNodeConfig);
        this.syncBuffer = new EditLogBuffer(nameNodeConfig);
    }

    /**
     * 写入一条editlog
     */
    public void write(EditLogWrapper editLog) throws IOException {
        currentBuffer.write(editLog);
    }

    /**
     * 交换两块缓冲区
     */
    public void setReadyToSync() {
        EditLogBuffer temp = currentBuffer;
        currentBuffer = syncBuffer;
        syncBuffer = temp;
    }

    /**
     * 把缓冲区的editlog数据刷新到磁盘
     */
    public EditslogInfo flush() throws IOException {
        EditslogInfo editslogInfo = syncBuffer.flush();
        if (editslogInfo != null) {
            syncBuffer.clear();
        }
        return editslogInfo;
    }

    /**
     * 是否可以刷新磁盘
     *
     * @return 是否可以刷磁盘
     */
    public boolean shouldForceSync() {
        return currentBuffer.size() >= nameNodeConfig.getEditLogFlushThreshold();
    }

    public List<EditLogWrapper> getCurrentEditLog() {
        return currentBuffer.getCurrentEditLog();
    }
}
