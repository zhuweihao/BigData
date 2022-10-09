package com.zhuweihao.DataStreamAPI;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Author zhuweihao
 * @Date 2022/10/9 15:17
 * @Description com.zhuweihao.DataStreamAPI
 */
public class KafkaSink {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","172.22.5.12:9092");

        DataStreamSource<String> streamSource = env.readTextFile("Flink/src/main/resources/clicks.csv");
        streamSource
                .addSink(new FlinkKafkaProducer<String>(
                        "clicks",
                        new SimpleStringSchema(),
                        properties
                ));
        env.execute();
    }
}
