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

/**
 * @author xjp
 * @create 2020-08-18 14:48
 */
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
                for (CanalEntry.Entry entry : entries) {
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
                            handle(tableName, eventType, rowDatasList);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    private static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        // 备注：求解交易额GMV只需要order_info表中的新增数据(insert类型)，因此需要进行判定
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                //a.构架json对象封装每行值；
                JSONObject jsonObject = new JSONObject();

                //b.获取所有的列名集合
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                for (CanalEntry.Column column : afterColumnsList) {
                    //c.逐行将列名和列值放进jsonobject对象中
                    jsonObject.put(column.getName(), column.getValue());
                }

                //d.打印单行数据并写入kafka中
                System.out.println(jsonObject.toString());

                MyKafkaSender.send(GmallConstants.GMALL_TOPIC_ORDER_INFO,jsonObject.toString());
            }
        }
    }
}
