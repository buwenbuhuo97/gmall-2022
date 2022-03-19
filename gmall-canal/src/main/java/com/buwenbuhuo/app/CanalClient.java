package com.buwenbuhuo.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.buwenbuhuo.constants.GmallConstants;
import com.buwenbuhuo.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Author 不温卜火
 * Create 2022-03-17 22:08
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: Canal客户端的代码实现
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        // 1.获取链接对象
        /**
         * 通过查看newSingleConnector源码，我们可以知道Canal即可单机又可以集群
         *  newSingleConnector四个参数如下：
         *      SocketAddress addresses：点击进入发现其为抽象类，找其实现方法InetSocketAddress
         *      String destination(消息目的地)：canal配置文件确定参数
         *      String username：没有设置账号密码，不需要填写，如果设置需要自己填写
         *      String password：
         */
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop01", 11111),
                "example", "", "");

        while (true){
            // 2. 获取连接
            connector.connect();
            // 3. 指定订阅的数据库  Tips:括号里面需要的是正则表达式
            /**
             * 如何确定subscribe内如何填写参数？
             *  点subscribe进去发现其为抽象方法，查找其实现类SimpleCanalConnector
             *  查找subscribe，翻到第八个确定其是要传入所要监听的数据库
             */
            connector.subscribe("gmall.*");
            // 4. 获取数据
            /**
             * 获取数据的方法有两种：get和getWithoutAck
             *  getWithoutAck：不指定 position 获取事件.
             *                 该方法返回的条件：
             *                 a. 拿够batchSize条记录或者超过timeout时间
             *                 b. 如果timeout=0，则阻塞至拿到batchSize记录才返回
             *                 canal 会记住此 client 最新的position。
             *                如果是第一次 fetch，则会从 canal 中保存的最老一条数据开始输出。
             *
             *  此测试demo不需要这么麻烦，此处选择get即可
             *                  该方法返回的条件：
             *                     a. 拿够batchSize条记录或者超过timeout时间
             *                     b. 如果timeout=0，则阻塞至拿到batchSize记录才返回
             */
            Message message = connector.get(100);

            // 5. 获取一个SQL执行的结果
            List<CanalEntry.Entry> entries = message.getEntries();

            // 如果有数据，直接拉取数据
            if (entries.size()>0){
                // 证明有数据存在
                for (CanalEntry.Entry entry : entries) {
                    // TODO 6. 获取表名
                    String tableName = entry.getHeader().getTableName();

                    // 7.获取entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    // 8.判断entry类型获取数据
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)){
                        // 9.获取序列化的数据
                        ByteString storeValue = entry.getStoreValue();
                        // 10.对数据做反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        // TODO 11. 获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        // TODO 12.获取具体的多行数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        // 根据不同的需求获取不同表中不同事件类型(新增|新增及变化|变化|删除)的数据
                        handle(tableName,eventType,rowDatasList);
                    }
                }
            }else{
                // 如果没数据的话，则休息一会在拉取数据
                System.out.println("没有数据休息一会^-^");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData>
            rowDatasList) {
        // 1. 根据表名判断获取的数据来源，根据事件类型判断获取新增的数据
        if ("order_info".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            // 获取每一行数据
            for (CanalEntry.RowData rowData : rowDatasList){
                // 获取每一行中每一列的数据
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : rowData.getAfterColumnsList()){
                    jsonObject.put(column.getName(), column.getValue());
                }
                System.out.println(jsonObject.toJSONString());
                // 将数据发送至Kafka
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());
            }
        }
    }
}
































