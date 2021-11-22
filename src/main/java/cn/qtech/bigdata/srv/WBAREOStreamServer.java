package cn.qtech.bigdata.srv;

import cn.qtech.bigdata.core.stream.WBAREOStreamEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WBAREOStreamServer {
    private static final Logger LOG = LoggerFactory.getLogger(WBAREOStreamServer.class);
    WBAREOStreamEngine streamEngine = new WBAREOStreamEngine();

    public void start() {

        try {
            streamEngine.start();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws Exception {

        WBAREOStreamServer streamSrv = new WBAREOStreamServer();
        streamSrv.start();
    }

}
