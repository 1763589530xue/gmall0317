package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static com.atguigu.constants.GmallConstants.GMALL_TOPIC_EVENT;
import static com.atguigu.constants.GmallConstants.GMALL_TOPIC_START;

/**
 * @author xjp
 * @create 2020-08-14 16:34
 */

@RestController
@Slf4j
public class LoggerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("log")
    public String getLogger(@RequestParam("logString") String logString) {
        //  添加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());

        //   写入日志
        log.info(jsonObject.toString());

        //   写入kafka
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GMALL_TOPIC_START, jsonObject.toString());
        } else {
            kafkaTemplate.send(GMALL_TOPIC_EVENT, jsonObject.toString());
        }
        return "success";
    }
}
