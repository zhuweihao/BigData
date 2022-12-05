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

        LineorderCDC(tableEnvironment);
        CustomerCDC(tableEnvironment);
        DatesCDC(tableEnvironment);
        SupplierCDC(tableEnvironment);
        PartCDC(tableEnvironment);
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
                "'topic'='lineorder-cdc'," +
                "'properties.bootstrap.servers'='172.22.5.12:9092'," +
                "'properties.group.id'='zwh'," +
                "'format'='debezium-json'" +
                ")"
        );
        tableEnvironment.executeSql("insert into lineorder_sink select * from lineorder_source");
    }

    public static void CustomerCDC(TableEnvironment tableEnvironment) {
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

    public static void DatesCDC(TableEnvironment tableEnvironment){
        tableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS `dates_source` (" +
                "  `d_datekey`int primary key not enforced," +
                "  `d_date` string ," +
                "  `d_dayofweek` string ," +
                "  `d_month` string ," +
                "  `d_year`int ," +
                "  `d_yearmonthnum`int ," +
                "  `d_yearmonth` string ," +
                "  `d_daynuminweek`int ," +
                "  `d_daynuminmonth`int ," +
                "  `d_daynuminyear`int ," +
                "  `d_monthnuminyear`int ," +
                "  `d_weeknuminyear`int ," +
                "  `d_sellingseason` string ," +
                "  `d_lastdayinweekfl`int ," +
                "  `d_lastdayinmonthfl`int ," +
                "  `d_holidayfl`int ," +
                "  `d_weekdayfl` int" +
                ") WITH (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='172.22.5.12'," +
                "'port'='3309'," +
                "'username'='root'," +
                "'password'='03283x'," +
                "'database-name'='ssb'," +
                "'table-name'='dates'" +
                ")");
        tableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS `dates_sink` (" +
                "  `d_datekey`int primary key not enforced," +
                "  `d_date` string ," +
                "  `d_dayofweek` string ," +
                "  `d_month` string ," +
                "  `d_year`int ," +
                "  `d_yearmonthnum`int ," +
                "  `d_yearmonth` string ," +
                "  `d_daynuminweek`int ," +
                "  `d_daynuminmonth`int ," +
                "  `d_daynuminyear`int ," +
                "  `d_monthnuminyear`int ," +
                "  `d_weeknuminyear`int ," +
                "  `d_sellingseason` string ," +
                "  `d_lastdayinweekfl`int ," +
                "  `d_lastdayinmonthfl`int ," +
                "  `d_holidayfl`int ," +
                "  `d_weekdayfl` int" +
                ") WITH (" +
                "'connector'='kafka'," +
                "'topic'='dates-cdc'," +
                "'properties.bootstrap.servers'='172.22.5.12:9092'," +
                "'properties.group.id'='zwh'," +
                "'format'='debezium-json'" +
                ")");
        tableEnvironment.executeSql("insert into dates_sink select * from dates_source");
    }

    public static void SupplierCDC(TableEnvironment tableEnvironment){
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

    public static void PartCDC(TableEnvironment tableEnvironment){
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
                "'topic'='part-source'," +
                "'properties.bootstrap.servers'='172.22.5.12:9092'," +
                "'properties.group.id'='zwh'," +
                "'format'='debezium-json'" +
                ")");
        tableEnvironment.executeSql("insert into part_sink select * from part_source");
    }
}
