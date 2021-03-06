package com.ruyuan.dfs.client.tools.command;

import com.ruyuan.dfs.client.FileSystem;
import org.jline.reader.LineReader;

/**
 * 实现PWD功能
 *
 * @author Sun Dasheng
 */
public class PwdCommand extends AbstractCommand {

    public PwdCommand(String currentPath, String command) {
        super(currentPath, command);
    }

    @Override
    public void execute(FileSystem fileSystem, LineReader lineReader) {
        System.out.println(currentPath);
    }
}
