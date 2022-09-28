package com.zhuweihao.DataStreamAPI;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

/**
 * @Author zhuweihao
 * @Date 2022/9/28 15:42
 * @Description com.zhuweihao.DataStreamAPI
 */
public class TransPOJOAggregation {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        streamSource.keyBy(e->e.user).max("timestamp").print();
        env.execute();
    }
}
