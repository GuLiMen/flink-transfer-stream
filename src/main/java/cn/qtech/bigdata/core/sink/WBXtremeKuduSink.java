package cn.qtech.bigdata.core.sink;

import cn.qtech.bigdata.comm.AppConstants;

import cn.qtech.bigdata.model.WBXtreme;
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
import java.util.*;

import static cn.qtech.bigdata.comm.AppConstants.RECEIVE_EMAIL;

public class WBXtremeKuduSink extends RichSinkFunction<Map<String, String>> {

    private String kuduMaster;
    private KuduSession kuduSession;
    private KuduTable kuduTable;
    private KuduClient kuduClient;
    private RedisOperations redisCli;
    private String tableName = AppConstants.KUDU_DATABASE + "." + AppConstants.WB_XTREME_KUDU_TABLE;
    private static final Logger LOG = LoggerFactory.getLogger(WBXtremeKuduSink.class);


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
            LOG.error(" process err!! ==err count [" + (++AppConstants.errCounter) + "]==WBKuduWBSink.invoke() input map == null");
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + "Job failed:", this.getClass().getName() + ".invoke() : invoke() input map == null \r\n" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\r\n");
            return;
        }
        //如果设备号(EQ...)是空  此条数据不存
        String deviceId = resultMap.getOrDefault("device_id", "");
        String create_date = resultMap.getOrDefault("create_date", "");

        if (deviceId == null || StringUtils.isBlank(deviceId) || create_date == null || StringUtils.isBlank(create_date)) {
            LOG.error("WBKuduWBSink: device_id id is null,device_id:{} || create_date id is null,create_date:{}" + deviceId + create_date);
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + "Job failed:", this.getClass().getName() + ".invoke()  device_id or create_date is null,device_id:{} \r\n" + resultMap.toString() + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\r\n");
            return;
        }
        try {
            String unitBondTotalTemp = resultMap.get("UnitBondTotal");
            String unitBondTotal = unitBondTotalTemp == null || StringUtils.isBlank(unitBondTotalTemp) ? "0" : unitBondTotalTemp.trim();
            String redisKey = AppConstants.WB_REDIS_KEY_PREFIX + deviceId;
            LocalTime parseTime = LocalTime.parse(create_date, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            if ((parseTime.getMinute() == 0 && parseTime.getSecond() < 10) || !unitBondTotal.equals(redisCli.get(redisKey))) {
                //整点,unitBondTotal 有变化 存入
                Boolean insertFlg = insertKudu(resultMap);
                if (!insertFlg) {
                    return;
                }

                /*
                 * redis存入:  key        expire   value
                 *            deviceId    1h       UnitBondTotal
                 * */
                redisCli.setex(redisKey, 3612, unitBondTotal);
            } else if (StringUtils.isBlank(redisCli.get(redisKey))) {

                Boolean insertFlg = insertKudu(resultMap);
                if (!insertFlg) {
                    return;
                }
                redisCli.setex(redisKey, 3612, unitBondTotal);
            }


        } catch (Exception e) {
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + "Job failed:", this.getClass().getName() + ".invoke() : insert failed !!数据插入异常 ! \r\n" + e.getStackTrace() + "\r\n" + resultMap.toString() + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\r\n" + e.getMessage());
            LOG.error(" 数据插入异常 ! WBKuduWBSink.invoke()", e);
        }

    }

    private Boolean insertKudu(Map<String, String> resultMap) {
        Insert insert = kuduTable.newInsert();
        PartialRow row = insert.getRow();
        //类型处理
        for (String fieldName : resultMap.keySet()) {
            String formatFieldName = fieldName.toLowerCase();
            String formatValue = resultMap.get(fieldName).trim();
            if ("lotname".equals(formatFieldName)) {
                int index1 = formatValue.indexOf("-");
                if (index1 > 0) {
                    if (formatValue.length() == index1 + 2) {
                        String s = String.valueOf(formatValue.charAt(index1 + 1));
                        if ("S".equals(s.toUpperCase()) || "1".equals(s) || "2".equals(s)) {
                            row.addString(formatFieldName, formatValue);
                            continue;
                        }
                    }
                    row.addString(formatFieldName, formatValue.substring(0, index1));
                    continue;
                }
                int index2 = formatValue.indexOf(" ");
                if (index2 > 0) {
                    row.addString(formatFieldName, formatValue.substring(0, index2));
                    continue;
                }

                int index3 = formatValue.indexOf("_");
                if (index3 > 0) {
                    row.addString(formatFieldName, formatValue.substring(0, index3));
                    continue;
                }
                int index4 = formatValue.indexOf(".");
                if (index4 > 0) {
                    row.addString(formatFieldName, formatValue.substring(0, index4));
                    continue;
                }
            }
            row.addString(formatFieldName, formatValue);
        }
        try {
            OperationResponse response = kuduSession.apply(insert);
            //检查插入数据是否失败
            if (response == null || kuduSession.countPendingErrors() == 0) {
                return true;
            }
        } catch (KuduException e) {
            SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + "Job failed:", this.getClass().getName() + ".insertKudu() : insert failed !! \r\n" + e.getStackTrace() + "\r\n" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\r\n" + e.getMessage() + resultMap.toString());
            LOG.error(" WBKuduWBSink.insertKudu() : insert failed !! ", e);
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
            LOG.error("关闭资源失败 ! WBKuduWBSink.close()", e);
        }
    }
}
