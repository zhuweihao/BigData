package com.zhuweihao.CDC;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Map;
import java.util.Properties;

/**
 * @Author zhuweihao
 * @Date 2022/12/6 11:02
 * @Description com.zhuweihao.CDC
 */
public class testkafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.22.5.15:9092");
        properties.setProperty("group.id", "flink-test");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserialize", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "earliest");
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>(
                "user_behavior",
                new SimpleStringSchema(),
                properties
        ));
//        stream.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String s) throws Exception {
//                Map map = JSON.parseObject(s);
//                Object after = map.get("after");
//                JSONObject jsonObject = JSON.parseObject(after.toString());
//                int c_custkey = jsonObject.getIntValue("lo_orderkey");
//                return c_custkey>=5999000;
//            }
//        }).print();
        stream.print();
        env.execute();
    }
}
