package com.zhuweihao.CDC;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhuweihao
 * @Date 2022/12/5 20:33
 * @Description com.zhuweihao.CDC
 */
public class PartCDC {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS `part_source` (" +
                "  `p_partkey` int primary key not enforced," +
                "  `p_name` string," +
                "  `p_mfgr` string," +
                "  `p_category` string," +
                "  `p_brand` string," +
                "  `p_color` string," +
                "  `p_type` string," +
                "  `p_size` int," +
                "  `p_container` string" +
                ") WITH (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='172.22.5.12'," +
                "'port'='3309'," +
                "'username'='root'," +
                "'password'='03283x'," +
                "'database-name'='ssb'," +
                "'table-name'='part'" +
                ")");
        tableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS `part_sink` (" +
                "  `p_partkey` int primary key not enforced," +
                "  `p_name` string," +
                "  `p_mfgr` string," +
                "  `p_category` string," +
                "  `p_brand` string," +
                "  `p_color` string," +
                "  `p_type` string," +
                "  `p_size` int," +
                "  `p_container` string" +
                ") WITH (" +
                "'connector'='kafka'," +
                "'topic'='part-cdc'," +
                "'properties.bootstrap.servers'='172.22.5.12:9092'," +
                "'properties.group.id'='zwh'," +
                "'format'='debezium-json'" +
                ")");
        tableEnvironment.executeSql("insert into part_sink select * from part_source");
    }
}
