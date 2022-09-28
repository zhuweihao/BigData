package com.zhuweihao.DataStreamAPI;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author zhuweihao
 * @Date 2022/9/28 14:53
 * @Description com.zhuweihao.DataStreamAPI
 */
public class TransKeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        //使用Lambda表达式
        KeyedStream<Event, String> keyedStream = streamSource.keyBy(e -> e.user);
        keyedStream.print();
        //使用匿名类实现KeySelector
        KeyedStream<Event, String> keyedStream1 = streamSource.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        });
        keyedStream1.print();
        env.execute();
    }
}
