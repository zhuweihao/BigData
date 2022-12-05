package com.zhuweihao.CDC;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhuweihao
 * @Date 2022/12/5 20:31
 * @Description com.zhuweihao.CDC
 */
public class DatesCDC {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
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
}
