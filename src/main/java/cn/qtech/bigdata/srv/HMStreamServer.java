package cn.qtech.bigdata.srv;

import cn.qtech.bigdata.core.stream.DBStreamEngine;
import cn.qtech.bigdata.core.stream.HMStreamEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HMStreamServer {
    private static final Logger LOG = LoggerFactory.getLogger(HMStreamServer.class);
    HMStreamEngine streamEngine = new HMStreamEngine();

    public void start() {

        try {
            streamEngine.start();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws Exception {
        HMStreamServer streamSrv = new HMStreamServer();
        streamSrv.start();
    }

}
