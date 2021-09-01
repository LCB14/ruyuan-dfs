package com.ruyuan.dfs.namenode.fs;

import com.ruyuan.dfs.backup.fs.FsImage;
import com.ruyuan.dfs.common.enums.FsOpType;
import com.ruyuan.dfs.common.metrics.Prometheus;
import com.ruyuan.dfs.common.utils.DefaultScheduler;
import com.ruyuan.dfs.model.backup.EditLog;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.datanode.DataNodeManager;
import com.ruyuan.dfs.namenode.editslog.EditLogWrapper;
import com.ruyuan.dfs.namenode.editslog.FsEditLog;
import com.ruyuan.dfs.namenode.server.UserManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 负责管理文件系统元数据的组件
 * <p>
 * 落地磁盘的实现
 *
 * @author Sun Dasheng
 */
@Slf4j
public class DiskNameSystem extends AbstractFsNameSystem {

    private NameNodeConfig nameNodeConfig;
    private FsEditLog editLog;

    public DiskNameSystem(NameNodeConfig nameNodeConfig, DefaultScheduler defaultScheduler,
                          DataNodeManager dataNodeManager, UserManager userManager) {
        super();
        this.nameNodeConfig = nameNodeConfig;
        this.editLog = new FsEditLog(nameNodeConfig);
        dataNodeManager.setDiskNameSystem(this);
        TrashPolicyDefault trashPolicyDefault = new TrashPolicyDefault(this, dataNodeManager, userManager);
        defaultScheduler.schedule("定时扫描物理删除文件", trashPolicyDefault,
                nameNodeConfig.getNameNodeTrashCheckInterval(),
                nameNodeConfig.getNameNodeTrashCheckInterval(), TimeUnit.MILLISECONDS);
    }

    public NameNodeConfig getNameNodeConfig() {
        return nameNodeConfig;
    }

    @Override
    public void recoveryNamespace() throws Exception {
        try {
            FsImage fsImage = scanLatestValidFsImage(nameNodeConfig.getBaseDir());
            long txId = 0L;
            if (fsImage != null) {
                txId = fsImage.getMaxTxId();
                applyFsImage(fsImage);
            }
            // 回放editLog文件
            this.editLog.playbackEditLog(txId, obj -> {
                EditLog editLog = obj.getEditLog();
                int opType = editLog.getOpType();
                if (opType == FsOpType.MKDIR.getValue()) {
                    // 这里要调用super.mkdir 回放的editLog不需要再刷磁盘
                    super.mkdir(editLog.getPath(), editLog.getAttrMap());
                } else if (opType == FsOpType.CREATE.getValue()) {
                    super.createFile(editLog.getPath(), editLog.getAttrMap());
                } else if (opType == FsOpType.DELETE.getValue()) {
                    super.deleteFile(editLog.getPath());
                }
            });
        } catch (Exception e) {
            log.info("NameNode恢复命名空间异常：", e);
            throw e;
        }
    }

    @Override
    public Node listFiles(String filename) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Node node = super.listFiles(filename);
        stopWatch.stop();
        Prometheus.gauge("namenode_fs_memory_cost", "FSDirectory操作耗时", "op", "listFiles", stopWatch.getTime());
        return node;
    }

    /**
     * 创建目录
     *
     * @param path 目录路径
     */
    @Override
    public void mkdir(String path, Map<String, String> attr) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        super.mkdir(path, attr);
        this.editLog.logEdit(new EditLogWrapper(FsOpType.MKDIR.getValue(), path, attr));
        log.info("创建文件夹：{}", path);
        stopWatch.stop();
        Prometheus.gauge("namenode_fs_memory_cost", "FSDirectory操作耗时", "op", "mkdir", stopWatch.getTime());
    }

    /**
     * 创建文件
     *
     * @param filename 文件路径
     */
    @Override
    public boolean createFile(String filename, Map<String, String> attr) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        if (!super.createFile(filename, attr)) {
            return false;
        }
        this.editLog.logEdit(new EditLogWrapper(FsOpType.CREATE.getValue(), filename, attr));
        Prometheus.gauge("namenode_fs_memory_cost", "FSDirectory操作耗时", "op", "createFile", stopWatch.getTime());
        return true;
    }

    @Override
    public boolean deleteFile(String filename) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        if (!super.deleteFile(filename)) {
            return false;
        }
        this.editLog.logEdit(new EditLogWrapper(FsOpType.DELETE.getValue(), filename));
        log.info("删除文件：{}", filename);
        Prometheus.gauge("namenode_fs_memory_cost", "FSDirectory操作耗时", "op", "deleteFile", stopWatch.getTime());
        return true;
    }

    /**
     * 优雅停机
     * 强制把内存里的edits log刷入磁盘中
     */
    public void shutdown() {
        log.info("Shutdown DiskNameSystem.");
        this.editLog.flush();
    }

    /**
     * 获取EditLog
     *
     * @return editLog
     */
    public FsEditLog getEditLog() {
        return editLog;
    }

}
