package com.zhuweihao.CDC;

import com.ibm.icu.impl.Row;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhuweihao
 * @Date 2022/12/1 14:29
 * @Description com.zhuweihao.CDC
 */
public class MySQLBinlogSourceSQL {
    public static void main(String[] args) throws Exception {
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
                "'topic'='mysql-cdc-kafka'," +
                "'properties.bootstrap.servers'='172.22.5.12:9092'," +
                "'properties.group.id'='zwh'," +
                "'format'='debezium-json'" +
                ")");

        tableEnvironment.executeSql("insert into supplier_sink select * from supplier_source");


    }

    public static void LineorderCDC(TableEnvironment tableEnvironment) {
        tableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS `lineorder_source` (" +
                "  `lo_orderkey` int PRIMARY KEY NOT ENFORCED," +
                "  `lo_linenumber` int ," +
                "  `lo_custkey` int ," +
                "  `lo_partkey` int ," +
                "  `lo_suppkey` int ," +
                "  `lo_orderdate` int ," +
                "  `lo_orderpriority` string ," +
                "  `lo_shippriority` int ," +
                "  `lo_quantity` int ," +
                "  `lo_extendedprice` int ," +
                "  `lo_ordtotalprice` int ," +
                "  `lo_discount` int ," +
                "  `lo_revenue` int ," +
                "  `lo_supplycost` int ," +
                "  `lo_tax` int ," +
                "  `lo_commitdate` int ," +
                "  `lo_shipmode` string " +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='172.22.5.12'," +
                "'port'='3309'," +
                "'username'='root'," +
                "'password'='03283x'," +
                "'database-name'='ssb'," +
                "'table-name'='lineorder'" +
                ")");
        tableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS `lineorder_sink` (" +
                "  `lo_orderkey` int PRIMARY KEY NOT ENFORCED," +
                "  `lo_linenumber` int ," +
                "  `lo_custkey` int ," +
                "  `lo_partkey` int ," +
                "  `lo_suppkey` int ," +
                "  `lo_orderdate` int ," +
                "  `lo_orderpriority` string ," +
                "  `lo_shippriority` int ," +
                "  `lo_quantity` int ," +
                "  `lo_extendedprice` int ," +
                "  `lo_ordtotalprice` int ," +
                "  `lo_discount` int ," +
                "  `lo_revenue` int ," +
                "  `lo_supplycost` int ," +
                "  `lo_tax` int ," +
                "  `lo_commitdate` int ," +
                "  `lo_shipmode` string " +
                ") with (" +
                "'connector'='kafka'," +
                "'topic'='mysql-cdc-kafka'," +
                "'properties.bootstrap.servers'='172.22.5.12:9092'," +
                "'properties.group.id'='zwh'," +
                "'format'='debezium-json'"
        );
        tableEnvironment.executeSql("insert into lineorder_sink from select * from lineorder_source");
    }

    public static void CustomerCDC(TableEnvironment tableEnvironment) {
        tableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS `customer_source` (" +
                "  `c_custkey` int," +
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
                "  `c_custkey` int," +
                "  `c_name` string," +
                "  `c_address` string," +
                "  `c_city` string," +
                "  `c_nation` string," +
                "  `c_region` string," +
                "  `c_phone` string," +
                "  `c_mktsegment` string" +
                ") WITH (" +
                "'connector'='kafka'," +
                "'topic'='mysql-cdc-kafka'," +
                "'properties.bootstrap.servers'='172.22.5.12:9092'," +
                "'properties.group.id'='zwh'," +
                "'format'='debezium-json'" +
                ")");
        tableEnvironment.executeSql("insert into customer_sink from select * from customer_source");
    }
}
