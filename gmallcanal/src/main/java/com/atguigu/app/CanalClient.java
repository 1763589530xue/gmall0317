package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;


import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;


public class CanalClient {
    public static void main(String[] args) {
        // 1.获取canal连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("warehousehadoop102", 11111),
                "example", "", "");

        // 2.对msg数据进行处理
        while (true) {

            //  a.获取连接
            canalConnector.connect();

            //  b.订阅要监控的表(gmall2020317数据库的所有表)
            canalConnector.subscribe("gmall200317.*");

            //  c.指定每次连接获取的信息数
            Message message = canalConnector.get(100);

            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size() == 0) {
                System.out.println("没有抓取到数据！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    // d.对Entry Type进行筛选，只保留rowdata类型的数据
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {

                        try {
                            String tableName = entry.getHeader().getTableName();
                            ByteString storeValues = entry.getStoreValue();
                            // e.对storeValues进行反序列化，使用RowChange类
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValues);

                            //f.获取事件类型
                            CanalEntry.EventType eventType = rowChange.getEventType();

                            //g.获取数据
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            //h.封装方法处理反序列化的数据
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

        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
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
            }

            //每条数据输出和写入kafka前均延迟几秒
           /* try {
                Thread.sleep(new Random().nextInt(2 * 1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            //将JSON对象打印出来并发送至kafka中
            System.out.println(jsonObject.toString());
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }
}
