package cn.qtech.bigdata.core.parser;

import cn.qtech.bigdata.model.WBAREO;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.HashMap;
import java.util.Map;


public class WBAREOParser extends RichMapFunction<String, Map<String, String>> {
    //private static final Logger LOG = LoggerFactory.getLogger(WBAREOParser.class);

    @Override
    public Map map(String in) throws Exception {

        JSONObject parseObj = JSON.parseObject(in);
        Map<String, String> resultMap = new HashMap<>();

        for (String fieldName : WBAREO.WBStringValue) {
            Object tmp = parseObj.get(fieldName);
            if (tmp == null || "N/A".equals(tmp) || "NONE".equals(tmp) || "null".equals(tmp) || "NULL".equals(tmp) || StringUtils.isBlank(tmp.toString())) {
                continue;
            }

            resultMap.put(fieldName, parseObj.get(fieldName).toString());
        }

        return resultMap;
    }
}
