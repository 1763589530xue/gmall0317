package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xjp
 * @create 2020-08-17 9:05
 */
public class MyKafkaConsumer {
    public static void main(String[] args) {

        Map<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "warehousehadoop102:9092,warehousehadoop103:9092,warehousehadoop104:9092");
//       shell中不用写消费者组(默认在配置指定有消费者组名)，但是此处API需要写；
        map.put(ConsumerConfig.GROUP_ID_CONFIG, "StartGroup");
        map.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(map);

//       定义消费主题
        ArrayList<String> list = new ArrayList<String>();
        list.add("Topic_Start");
        kafkaConsumer.subscribe(list);

//       需要长轮询，主动去topic中拉取数据；
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset=%d,key=%s,value=%s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}




















