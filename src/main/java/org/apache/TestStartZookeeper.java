package org.apache;

import org.apache.zookeeper.server.quorum.QuorumPeerMain;

/**
 * Created by dukangli on 2019/6/25 10:23
 */
public class TestStartZookeeper {
    public static void main(String[] args) {
        String confPath = "D:\\developer\\zookeeper-3.4.13\\conf\\zoo3.cfg";
        QuorumPeerMain.main(new String[]{confPath});
    }
}
