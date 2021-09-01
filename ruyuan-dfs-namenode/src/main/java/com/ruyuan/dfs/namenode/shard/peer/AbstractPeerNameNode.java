package com.ruyuan.dfs.namenode.shard.peer;

/**
 * 抽象的PeerNameNode
 *
 * @author Sun Dasheng
 */
public abstract class AbstractPeerNameNode implements PeerNameNode {
    private String server;
    protected int currentNodeId;
    private int targetNodeId;

    public AbstractPeerNameNode(int currentNodeId, int targetNodeId, String server) {
        this.currentNodeId = currentNodeId;
        this.targetNodeId = targetNodeId;
        this.server = server;
    }

    @Override
    public int getTargetNodeId() {
        return targetNodeId;
    }

    @Override
    public String getServer() {
        return server;
    }
}