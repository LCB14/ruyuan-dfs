package com.ruyuan.dfs.backup.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 配置文件
 *
 * @author Sun Dasheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class BackupNodeConfig {

    public static final String FS_IMAGE_NAME = "fsimage-%s";
    private static final  Pattern PATTERN = Pattern.compile("(\\S+):(\\S+)");

    private String baseDir;
    private long fetchEditLogInterval;
    private int fetchEditLogSize;
    private long checkpointInterval;
    private String nameNodeServer;
    private String backupNodeServer;

    public static BackupNodeConfig parse(Properties properties) {
        String baseDir = (String) properties.get("base.dir");
        long fetchEditLogInterval = Integer.parseInt((String) properties.get("fetch.editslog.interval"));
        int fetchEditLogSize = Integer.parseInt((String) properties.get("fetch.editslog.size"));
        long checkpointInterval = Long.parseLong((String) properties.get("checkpoint.interval"));
        String nameNodeServer = (String) properties.get("namenode.server");
        String backupNodeServer = (String) properties.get("backupnode.server");
        return BackupNodeConfig.builder()
                .baseDir(baseDir)
                .fetchEditLogInterval(fetchEditLogInterval)
                .fetchEditLogSize(fetchEditLogSize)
                .checkpointInterval(checkpointInterval)
                .nameNodeServer(nameNodeServer)
                .backupNodeServer(backupNodeServer)
                .build();
    }

    public String getFsImageFile(String time) {
        return baseDir + File.separator + String.format(FS_IMAGE_NAME, time);
    }

    public String getNameNodeHostname() {
        Matcher matcher = PATTERN.matcher(nameNodeServer);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    public int getNameNodePort() {
        Matcher matcher = PATTERN.matcher(nameNodeServer);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(2));
        }
        return 0;
    }

    public String getBackupNodeHostname() {
        Matcher matcher = PATTERN.matcher(backupNodeServer);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    public int getBackupNodePort() {
        Matcher matcher = PATTERN.matcher(backupNodeServer);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(2));
        }
        return 0;
    }
}
