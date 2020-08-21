package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaProducer1;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;


/**
 * @author xjp
 * @create 2020-08-19 20:25
 */
public class CanalClient1 {
    public static void main(String[] args) {

       /* CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("warehousehadoop102", 11111),
                "example",
                "",
                "");

        while (true) {  //此处canal需要长轮询不断地监视mysql中数据的变化情况
            canalConnector.connect();
            canalConnector.subscribe("gmall200317.*");  //设置要监控的表

            Message message = canalConnector.get(100);

            if (message == null) {
                System.out.println("未抓取到数据！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                List<CanalEntry.Entry> entries = message.getEntries();
                for (CanalEntry.Entry entry : entries) {

                    CanalEntry.EntryType entryType = entry.getEntryType();
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        try {
                            String tableName = entry.getHeader().getTableName();
                            ByteString storeValue = entry.getStoreValue();

                            //进行反序列化
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                            //获取事件类型
                            CanalEntry.EventType eventType = rowChange.getEventType();

                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            //封装方法处理数据
                            handler(tableName, eventType, rowDatasList);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        // 先同时判断表名是否为order_info以及eventType是否为insert，求Gmv只用关心order_info表的新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            // 将满足要求的数据写入kafka指定主题中
            sendToKafka(rowDatasList, GmallConstants.GMALL_TOPIC_ORDER_INFO);
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, GmallConstants.GMALL_TOPIC_ORDER_DETAIL);
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType)|| CanalEntry.EventType.UPDATE.equals(eventType))) {
            sendToKafka(rowDatasList, GmallConstants.GMALL_TOPIC_USER_INFO);
        }
    }


    // 将insert命令操作的数据写入kafka中
    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            // 创建一个JSON对象用于存放一行数据(此时直接用JSON.parseObject不起作用)
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());

                //将JSON对象打印出来并发送至kafka中
                System.out.println(jsonObject.toString());
                MyKafkaProducer1.send(topic, jsonObject);
            }
        }*/
    }
}
