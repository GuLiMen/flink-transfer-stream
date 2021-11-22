package cn.qtech.bigdata.core.sink;

import cn.qtech.bigdata.comm.AppConstants;
import cn.qtech.bigdata.util.RedisOperations;
import cn.qtech.bigdata.util.SendEMailWarning;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;

import static cn.qtech.bigdata.comm.AppConstants.RECEIVE_EMAIL;

public class YGuKuduSink extends RichSinkFunction<Map<String, String>> {

    private String kuduMaster;
    private KuduSession kuduSession;
    private KuduTable kuduTable;
    private KuduClient kuduClient;
    private RedisOperations redisCli;
    private String tableName = AppConstants.KUDU_DATABASE + "." + AppConstants.YGu_KUDU_TABLE;
    private static final Logger LOG = LoggerFactory.getLogger(YGuKuduSink.class);


    @Override
    public void open(Configuration parameters) throws Exception {
        redisCli = new RedisOperations();
        kuduMaster = AppConstants.KUDU_MASTER;
        kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build();
        kuduTable = kuduClient.openTable(tableName);
        kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    }

    @Override
    public void invoke(Map<String, String> resultMap, Context context) throws Exception {
        if (resultMap == null) {
            LOG.error(" process err!! ==err count [" + (++AppConstants.errCounter) + "]==YGuKuduSink.invoke() input map == null");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + "Job failed:", this.getClass().getName() + ".invoke() : invoke() input map == null \r\n" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\r\n");
            return;
        }
        //如果 设备号,时间 是空  此条数据不存
        String deviceId = resultMap.getOrDefault("device_id", "");
        String create_date = resultMap.getOrDefault("create_date", "");

        if (deviceId == null || StringUtils.isBlank(deviceId) || create_date == null || StringUtils.isBlank(create_date)) {
            LOG.error(" YGuKuduSink: device_id id is null,device_id:{} || create_date id is null,create_date:{}" + deviceId + create_date);
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + "Job failed:", this.getClass().getName() + ".invoke()  device_id id is null,device_id:{} \r\n" + resultMap.toString() + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\r\n");
            return;
        }
        try {
            String unitBondTotalTemp = resultMap.get("pass");
            String unitBondTotal = unitBondTotalTemp == null || StringUtils.isBlank(unitBondTotalTemp) ? "0" : unitBondTotalTemp.trim();
            String redisKey = AppConstants.YGu_REDIS_KEY_PREFIX + deviceId;
            LocalTime parse = LocalTime.parse(create_date, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            if ((parse.getMinute() == 0 && parse.getSecond() < 9) || !unitBondTotal.equals(redisCli.get(redisKey))) {
                //整点,unitBondTotal 有变化 存入
                Boolean insertFlg = insertKudu(resultMap);
                if (!insertFlg) {
                    return;
                }

                /*
                 * redis存入:  key        expire   value
                 *            deviceId    1h       UnitBondTotal
                 * */
                redisCli.setex(redisKey, 3600, unitBondTotal);
            } else if (StringUtils.isBlank(redisCli.get(redisKey))) {

                Boolean insertFlg = insertKudu(resultMap);
                if (!insertFlg) {
                    return;
                }
                redisCli.setex(redisKey, 3600, unitBondTotal);
            }


        } catch (Exception e) {
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + "Job failed:", this.getClass().getName() + ".invoke() : insert failed !!数据插入异常 ! \r\n" + e.getStackTrace() + "\r\n" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\r\n" + e.getMessage());
            LOG.error(" 数据插入异常 ! YGuKuduSink.invoke()", e);
        }

    }

    private Boolean insertKudu(Map<String, String> resultMap) {
        Insert insert = kuduTable.newInsert();
        PartialRow row = insert.getRow();

        //类型处理
        for (String fieldName : resultMap.keySet()) {
            row.addString(fieldName.toLowerCase(), resultMap.get(fieldName));

        }
        try {
            OperationResponse response = kuduSession.apply(insert);
            //检查插入数据是否失败
            if (response == null || kuduSession.countPendingErrors() == 0) {
                return true;
            }

        } catch (KuduException e) {
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + "Job failed:", this.getClass().getName() + ".insertKudu() : insert failed !! \r\n" + e.getStackTrace() + "\r\n" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\r\n" + e.getMessage());
            LOG.error(" YGuKuduSink.insertKudu() : insert failed !! ", e);
        }
        return false;

    }


    @Override
    public void close() throws Exception {

        try {
            if (kuduSession != null) {
                kuduSession.close();
            }
            if (kuduClient != null) {
                kuduClient.close();
            }
        } catch (Exception e) {
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + "Job failed:", this.getClass().getName() + ".close() :关闭资源失败 ! !! \r\n" + e.getStackTrace() + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + e.getMessage() + "\r\n");
            LOG.error("关闭资源失败 ! YGuKuduSink.close()", e);
        }
    }
}
