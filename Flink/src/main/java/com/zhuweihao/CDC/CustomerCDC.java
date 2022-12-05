package com.zhuweihao.CDC;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhuweihao
 * @Date 2022/12/5 20:31
 * @Description com.zhuweihao.CDC
 */
public class CustomerCDC {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS `customer_source` (" +
                "  `c_custkey` int primary key not enforced," +
                "  `c_name` string," +
                "  `c_address` string," +
                "  `c_city` string," +
                "  `c_nation` string," +
                "  `c_region` string," +
                "  `c_phone` string," +
                "  `c_mktsegment` string" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='172.22.5.12'," +
                "'port'='3309'," +
                "'username'='root'," +
                "'password'='03283x'," +
                "'database-name'='ssb'," +
                "'table-name'='customer'" +
                ")");
        tableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS `customer_sink` (" +
                "  `c_custkey` int primary key not enforced," +
                "  `c_name` string," +
                "  `c_address` string," +
                "  `c_city` string," +
                "  `c_nation` string," +
                "  `c_region` string," +
                "  `c_phone` string," +
                "  `c_mktsegment` string" +
                ") WITH (" +
                "'connector'='kafka'," +
                "'topic'='customer-cdc'," +
                "'properties.bootstrap.servers'='172.22.5.12:9092'," +
                "'properties.group.id'='zwh'," +
                "'format'='debezium-json'" +
                ")");
        tableEnvironment.executeSql("insert into customer_sink select * from customer_source");
    }
}
