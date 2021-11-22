package cn.qtech.bigdata.srv;

import cn.qtech.bigdata.core.stream.WBXtremeStreamEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WBXtremeStreamServer {
    private static final Logger LOG = LoggerFactory.getLogger(WBXtremeStreamServer.class);
    WBXtremeStreamEngine streamEngine = new WBXtremeStreamEngine();

    public void start() {

        try {
            streamEngine.start();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws Exception {

        WBXtremeStreamServer streamSrv = new WBXtremeStreamServer();
        streamSrv.start();
    }

}
