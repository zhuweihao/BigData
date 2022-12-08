package com.zhuweihao.CDC;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.zhuweihao.Utils.CustomerJsonDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Optional;
import java.util.Properties;

/**
 * @Author zhuweihao
 * @Date 2022/11/30 20:47
 * @Description com.zhuweihao.CDC
 */
public class MySqlBinlogSource {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("172.22.5.12")
                .port(3309)
                .databaseList("ssb") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("ssb.supplier") // 设置捕获的表
                .username("root")
                .password("03283x")
                .deserializer(new CustomerJsonDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("172.22.5.12:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("mysql-cdc-kafka")
                        .setKeySerializationSchema(new SimpleStringSchema())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
//        env
//                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//                // 设置 source 节点的并行度为 4
//                .setParallelism(4)
//                .sinkTo(kafkaSink)
//                .setParallelism(1);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(4)
                .print().setParallelism(1); // 设置 sink 节点并行度为 1
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "172.22.5.12:9092");
//        env
//                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//                .setParallelism(4)
//                .addSink(new FlinkKafkaProducer<String>(
//                        "mysql-cdc-kafka",
//                        new SimpleStringSchema(),
//                        properties
//                ))
//                .setParallelism(1);


        env.execute("Print MySQL Snapshot + Binlog");

    }
}
