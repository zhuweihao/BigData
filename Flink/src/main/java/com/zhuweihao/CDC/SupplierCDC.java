package com.zhuweihao.CDC;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhuweihao
 * @Date 2022/12/5 20:32
 * @Description com.zhuweihao.CDC
 */
public class SupplierCDC {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("CREATE TABLE `supplier_source`(" +
                "`s_suppkey` INT PRIMARY KEY NOT ENFORCED," +
                "`s_name` STRING," +
                "`s_address` STRING," +
                "`s_city` STRING," +
                "`s_nation` STRING," +
                "`s_region` STRING," +
                "`s_phone` STRING" +
                ") WITH (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='172.22.5.12'," +
                "'port'='3309'," +
                "'username'='root'," +
                "'password'='03283x'," +
                "'database-name'='ssb'," +
                "'table-name'='supplier'" +
                ")");
        tableEnvironment.executeSql("CREATE TABLE `supplier_sink`(" +
                "`s_suppkey` INT PRIMARY KEY NOT ENFORCED," +
                "`s_name` STRING," +
                "`s_address` STRING," +
                "`s_city` STRING," +
                "`s_nation` STRING," +
                "`s_region` STRING," +
                "`s_phone` STRING" +
                ") WITH (" +
                "'connector'='kafka'," +
                "'topic'='supplier-cdc'," +
                "'properties.bootstrap.servers'='172.22.5.12:9092'," +
                "'properties.group.id'='zwh'," +
                "'format'='debezium-json'" +
                ")");

        tableEnvironment.executeSql("insert into supplier_sink select * from supplier_source");
    }
}
