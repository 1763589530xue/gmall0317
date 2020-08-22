package com.atguigu.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;


public class MyKafkaSender {

    public static KafkaProducer<String, String> kafkaProducer = null;

    public static KafkaProducer<String, String> getKafkaProducer() {

        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "warehousehadoop102:9092,warehousehadoop103:9092,warehousehadoop104:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(prop);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return producer;
    }

    public static void send(String topic, String msg) {
        if (kafkaProducer == null) {
            kafkaProducer = getKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<>(topic, msg));
    }
}
