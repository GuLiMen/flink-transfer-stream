package cn.qtech.bigdata.srv;

import cn.qtech.bigdata.core.stream.AAStreamEngine;
import cn.qtech.bigdata.core.stream.DBStreamEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AAStreamServer {
    private static final Logger LOG = LoggerFactory.getLogger(AAStreamServer.class);
    AAStreamEngine streamEngine = new AAStreamEngine();

    public void start() {

        try {
            streamEngine.start();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws Exception {
        AAStreamServer streamSrv = new AAStreamServer();
        streamSrv.start();
    }

}
