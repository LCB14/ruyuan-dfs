package com.ruyuan.dfs.namenode.server;

import com.ruyuan.dfs.common.network.file.FileAttribute;
import com.ruyuan.dfs.common.network.file.FileTransportCallback;
import com.ruyuan.dfs.common.utils.DefaultScheduler;
import com.ruyuan.dfs.namenode.config.NameNodeConfig;
import com.ruyuan.dfs.namenode.fs.DiskNameSystem;
import com.ruyuan.dfs.namenode.fs.FsImageClearTask;

/**
 * FsImage文件接受回调
 *
 * @author Sun Dasheng
 */
public class FsImageFileTransportCallback implements FileTransportCallback {

    private FsImageClearTask fsImageClearTask;
    private DefaultScheduler defaultScheduler;
    private NameNodeConfig nameNodeConfig;

    public FsImageFileTransportCallback(NameNodeConfig nameNodeConfig, DefaultScheduler defaultScheduler, DiskNameSystem diskNameSystem) {
        this.nameNodeConfig = nameNodeConfig;
        this.defaultScheduler = defaultScheduler;
        this.fsImageClearTask = new FsImageClearTask(diskNameSystem, nameNodeConfig.getBaseDir(),
                diskNameSystem.getEditLog());
    }


    @Override
    public String getPath(String filename) {
        // 这里只接收FSImage文件，文件名写死就可以了
        return nameNodeConfig.getFsimageFile(String.valueOf(System.currentTimeMillis()));
    }

    @Override
    public void onCompleted(FileAttribute fileAttribute) {
        defaultScheduler.scheduleOnce("删除FSImage任务", fsImageClearTask, 0);
    }
}
