package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

/**
 * @author xjp
 * @create 2020-08-19 21:08
 */

// 用于将CanalClient1中包装好的json对象发送到指定主题
public class MyKafkaProducer1 {

    private static KafkaProducer<String, String> kafkaProducer = null;

    public static KafkaProducer<String, String> getKafkaProducer() {

        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "warehousehadoop102:9092,warehousehadoop103:9092,warehousehadoop104:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        kafkaProducer = new KafkaProducer<>(prop);
        return kafkaProducer;
    }

    public static void send(String topic, JSONObject jsonObject) {
        if (kafkaProducer == null)
            getKafkaProducer();
        kafkaProducer.send(new ProducerRecord<String, String>(topic, jsonObject.toString()));
    }
}
