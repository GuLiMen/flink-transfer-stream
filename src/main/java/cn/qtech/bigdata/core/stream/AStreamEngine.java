package cn.qtech.bigdata.core.stream;

public abstract class AStreamEngine {

    abstract void initConifg();

    abstract void srvConsumeQueueData() throws Exception ;

    public void start() throws Exception {
        this.initConifg();
        this.srvConsumeQueueData();
    }
}
