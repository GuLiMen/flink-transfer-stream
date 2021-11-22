package cn.qtech.bigdata.core.stream;

import cn.qtech.bigdata.comm.AppConstants;
import cn.qtech.bigdata.core.parser.WBAREOParser;
import cn.qtech.bigdata.core.sink.WBAREOKuduSink;
import cn.qtech.bigdata.model.WBEquipment;
import cn.qtech.bigdata.prop.PropertiesManager;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class WBAREOStreamEngine extends AStreamEngine {
    private static final Logger LOG = LoggerFactory.getLogger(WBAREOStreamEngine.class);
    private PropertiesManager pm = PropertiesManager.getInstance();


    @Override
    void initConifg() {
        try {
            LOG.info("==========加载配置文件WB-AREO-stream-srv===========");
            PropertiesManager.loadProps("/WB-AREO-stream-srv.properties");
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    void srvConsumeQueueData() throws Exception {
        String topicName = pm.getString(AppConstants.consumer_kafka_topic);
        Integer parallelism = pm.getInt(AppConstants.parallelism);
        String kafkaAddress = pm.getString(AppConstants.kafka_address);
        // 1.获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);// 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setCheckpointTimeout(120000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(40, TimeUnit.SECONDS)));
        // 取消作业时保存检查点。请注意，在这种情况下，在取消后手动清理检查点状态。
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
       // env.setStateBackend(new FsStateBackend(new Path("hdfs://nameservice/flink/flink-checkpoints/WBAREOStreamEngine")));
        env.setParallelism(parallelism);
        // 2.建立Kafka连接
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaAddress);
        properties.setProperty("group.id", "WBAREOServer2021");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), properties);

        // 3.获取Kafka数据
        consumer.setStartFromGroupOffsets();
        env.addSource(consumer).uid("WBAReOSource")
                .filter(x -> "WB".equals(JSON.parseObject(x).getString("device_type")) || "wb".equals(JSON.parseObject(x).getString("device_type"))).uid("WBAREOFilterDevice_type")
                .filter(x -> WBEquipment.WBAREODevice.contains(JSON.parseObject(x).getString("device_id").trim())).uid("WBAREOFilterDevice_id")
                .map(new WBAREOParser()).uid("WBAREOMap")
                .addSink(new WBAREOKuduSink()).uid("WBAREOSink");


        env.execute(WBAREOStreamEngine.class.getSimpleName());
    }
}
