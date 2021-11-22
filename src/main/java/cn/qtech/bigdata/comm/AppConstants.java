package cn.qtech.bigdata.comm;



import java.util.Arrays;
import java.util.List;


public class AppConstants {
    /**
     * 消费采集后 传入kafka的数据
     * kafka 相关配置
     */
    public static String consumer_kafka_topic = "kafka.topic.consume";
    public static String kafka_address = "bootstrap.servers";
    public static String parallelism = "flink.parallelism";
    public static final String KUDU_MASTER = "bigdata01:7051,bigdata02:7051,bigdata03:7051";

    /**
     * WB DB 数据采集后写入 kudu库表
     */
    public static final String KUDU_DATABASE = "impala::ods_machine_extract";
    public static final String WB_XTREME_KUDU_TABLE = "WB_Xtreme";
    public static final String WB_AERO_KUDU_TABLE = "WB_AREO";
    public static final String DB_KUDU_TABLE = "DB";
    public static final String  HM_KUDU_TABLE="HM";
    public static final String  AA_KUDU_TABLE="AA";
    public static final String  YGu_KUDU_TABLE="YGu";

    //临时用-统计TPS
    public static volatile int errCounter;

    /**
     * redis 配置
     */
    public static final String REIDS_HOST = "10.170.6.135";
    public static final int REIDS_PORT = 6379;
    public static final String REDIS_PASSWD = "";
    public static final String WB_REDIS_KEY_PREFIX = "WBxtreme";
    public static final String WB_AERO_REDIS_KEY_PREFIX = "WBAREO";
    public static final String DB_REDIS_KEY_PREFIX = "DB";
    public static final String HM_REDIS_KEY_PREFIX = "HM";
    public static final String AA_REDIS_KEY_PREFIX = "AA";
    public static final String YGu_REDIS_KEY_PREFIX = "YGu";

    /**
     * job失败接收邮箱
     */
    public final static List<String> RECEIVE_EMAIL= Arrays.asList("limeng.gu@qtechglobal.com", "liqin.liu@qtechglobal.com");

}
