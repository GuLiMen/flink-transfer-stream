package cn.qtech.bigdata.srv;

import cn.qtech.bigdata.core.stream.DBStreamEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBStreamServer {
    private static final Logger LOG = LoggerFactory.getLogger(DBStreamServer.class);
    DBStreamEngine streamEngine = new DBStreamEngine();

    public void start() {

        try {
            streamEngine.start();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws Exception {
        DBStreamServer streamSrv = new DBStreamServer();
        streamSrv.start();
    }

}
