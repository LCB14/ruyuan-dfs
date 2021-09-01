package com.ruyuan.dfs.namenode.fs;

import com.ruyuan.dfs.namenode.editslog.EditLogWrapper;

/**
 * 回放
 *
 * @author Sun Dasheng
 */
public interface PlaybackEditLogCallback {

    /**
     * 回放
     *
     * @param editLogWrapper editLogWrapper
     */
    void playback(EditLogWrapper editLogWrapper);
}
