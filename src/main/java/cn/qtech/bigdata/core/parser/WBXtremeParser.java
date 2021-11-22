package cn.qtech.bigdata.core.parser;

import cn.qtech.bigdata.model.WBXtreme;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;


import java.util.HashMap;
import java.util.Map;


public class WBXtremeParser extends RichMapFunction<String, Map<String, String>> {
    // private static final Logger LOG = LoggerFactory.getLogger(WBXtremeParser.class);

    @Override
    public Map map(String in) throws Exception {

        JSONObject parseObj = JSON.parseObject(in);
        Map<String, String> resultMap = new HashMap<>();
        for (String fieldName : WBXtreme.WBStringValue) {
            Object tmp = parseObj.get(fieldName);
            if (tmp == null || "N/A".equals(tmp) || "NONE".equals(tmp) || "null".equals(tmp) || "NULL".equals(tmp) || StringUtils.isBlank(tmp.toString())) {
                continue;
            }

            resultMap.put(fieldName, parseObj.get(fieldName).toString());
        }
/*        for (String fieldName : parseObj.keySet()) {
            Object tmp = parseObj.getOrDefault(fieldName, "");
            if (tmp == null || "N/A".equals(tmp) || "NONE".equals(tmp) || "null".equals(tmp) || "NULL".equals(tmp) || StringUtils.isBlank(tmp.toString()) || fieldName == null) {
                // SendEMailWarning.sendMail(RECEIVE_EMAIL, this.getClass().getName() + "Job failed:", this.getClass().getName() + ".map() : insert failed !!数据插入异常 ! \r\n"  + "\r\n" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\r\n" );
                //   LOG.warn(" 数据为空 ! "+fieldName+"="+tmp);
                continue;
            }

            resultMap.put(fieldName, parseObj.get(fieldName).toString());
        }*/

        return resultMap;
    }
}
