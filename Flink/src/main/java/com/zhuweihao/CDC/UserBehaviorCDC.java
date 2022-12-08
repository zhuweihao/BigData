package com.zhuweihao.CDC;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhuweihao
 * @Date 2022/12/8 15:14
 * @Description com.zhuweihao.CDC
 */
public class UserBehaviorCDC {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.enableCheckpointing(3000);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        tableEnvironment.executeSql("CREATE TABLE `userbehavior_01_source` (" +
                "  `user_id` int NOT NULL," +
                "  `item_id` int NOT NULL," +
                "  `category_id` int NOT NULL," +
                "  `behavior_type` string not null," +
                "  `timestamp` int NOT NULL," +
                "PRIMARY KEY (`user_id`,`item_id`,`timestamp`) not enforced" +
                ") with(" +
                "'connector'='mysql-cdc'," +
                "'hostname'='172.22.5.12'," +
                "'port'='3309'," +
                "'username'='root'," +
                "'password'='03283x'," +
                "'database-name'='UserBehavior'," +
                "'table-name'='userbehavior_01'" +
                ")");
        tableEnvironment.executeSql("CREATE TABLE `userbehavior_01_sink`  (" +
                "  `user_id` int NOT NULL," +
                "  `item_id` int NOT NULL," +
                "  `category_id` int NOT NULL," +
                "  `behavior_type` string not null," +
                "  `timestamp` int NOT NULL," +
                "PRIMARY KEY (`user_id`,`item_id`,`timestamp`) not enforced" +
                ") with (" +
                "'connector'='kafka'," +
                "'topic'='userbehavior_01-cdc'," +
                "'properties.bootstrap.servers'='172.22.5.12:9092'," +
                "'properties.group.id'='zwh'," +
                "'format'='debezium-json'" +
                ")");
        tableEnvironment.executeSql("insert into `userbehavior_01_sink` select * from `userbehavior_01_source`");
    }
}
