package cn.qtech.bigdata.srv;

import cn.qtech.bigdata.core.stream.DBStreamEngine;
import cn.qtech.bigdata.core.stream.YGuStreamEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YGuStreamServer {
    private static final Logger LOG = LoggerFactory.getLogger(YGuStreamServer.class);
    YGuStreamEngine streamEngine = new YGuStreamEngine();

    public void start() {

        try {
            streamEngine.start();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws Exception {
        YGuStreamServer streamSrv = new YGuStreamServer();
        streamSrv.start();
    }

}
