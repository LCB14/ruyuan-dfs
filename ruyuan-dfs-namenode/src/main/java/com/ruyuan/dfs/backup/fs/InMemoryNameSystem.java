package com.ruyuan.dfs.backup.fs;

import com.ruyuan.dfs.backup.config.BackupNodeConfig;
import com.ruyuan.dfs.namenode.fs.AbstractFsNameSystem;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于内存的文件系统
 *
 * @author Sun Dasheng
 */
@Slf4j
public class InMemoryNameSystem extends AbstractFsNameSystem {

    private BackupNodeConfig backupNodeConfig;
    private volatile long maxTxId = 0L;
    private AtomicBoolean recovering = new AtomicBoolean(false);

    public InMemoryNameSystem(BackupNodeConfig backupNodeConfig) {
        this.backupNodeConfig = backupNodeConfig;

    }

    public long getMaxTxId() {
        return maxTxId;
    }

    /**
     * 设置当前最大的TxId
     *
     * @param maxTxId TxId
     */
    public void setMaxTxId(Long maxTxId) {
        this.maxTxId = maxTxId;
    }

    @Override
    public void recoveryNamespace() throws Exception {
        try {
            if (recovering.compareAndSet(false, true)) {
                FsImage fsImage = scanLatestValidFsImage(backupNodeConfig.getBaseDir());
                if (fsImage != null) {
                    setMaxTxId(fsImage.getMaxTxId());
                    applyFsImage(fsImage);
                }
                recovering.compareAndSet(true, false);
            }
        } catch (Exception e) {
            log.info("BackupNode恢复命名空间异常：", e);
            throw e;
        }
    }

    /**
     * 恢复过程是否完成
     */
    public boolean isRecovering() {
        return recovering.get();
    }

    /**
     * 获取FSImage
     *
     * @return FsImage
     */
    public FsImage getFsImage() {
        FsImage fsImage = directory.getFsImage();
        fsImage.setMaxTxId(maxTxId);
        return fsImage;
    }

}
