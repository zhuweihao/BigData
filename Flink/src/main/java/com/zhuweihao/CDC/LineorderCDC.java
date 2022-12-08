package com.zhuweihao.CDC;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhuweihao
 * @Date 2022/12/5 20:28
 * @Description com.zhuweihao.CDC
 */
public class LineorderCDC {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(12);
        env.enableCheckpointing(3000);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS `lineorder_source` (" +
                "  `lo_orderkey` int ," +
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
                "  `lo_shipmode` string ," +
                "PRIMARY KEY (lo_orderkey,lo_linenumber) NOT ENFORCED" +
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
                "  `lo_orderkey` int," +
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
                "  `lo_shipmode` string ," +
                "PRIMARY KEY (lo_orderkey,lo_linenumber) NOT ENFORCED" +
                ") with (" +
                "'connector'='kafka'," +
                "'topic'='lineorder-cdc'," +
                "'properties.bootstrap.servers'='172.22.5.12:9092'," +
                "'properties.group.id'='zwh'," +
                "'format'='debezium-json'" +
                ")"
        );
        tableEnvironment.executeSql("insert into lineorder_sink select * from lineorder_source");
    }
}
