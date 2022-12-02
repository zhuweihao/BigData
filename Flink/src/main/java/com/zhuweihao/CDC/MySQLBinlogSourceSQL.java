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
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                //.inBatchMode()
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        tableEnvironment.executeSql("SET execution.checkpointing.interval = 3s");

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
                "'connector'='upsert-kafka'," +
                "'topic'='mysql-cdc-kafka'," +
                "'properties.bootstrap.servers'='172.22.5.12:9092'," +
                "'properties.group.id'='zwh'," +
                "'key.format'='csv'," +
                "'value.format'='csv'" +
                ")");

        tableEnvironment.executeSql("insert into supplier_sink select * from supplier_source");

    }
}
