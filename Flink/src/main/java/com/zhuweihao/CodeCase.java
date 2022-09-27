package com.zhuweihao;

import com.zhuweihao.DataStreamAPI.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author zhuweihao
 * @Date 2022/9/27 16:25
 * @Description com.zhuweihao
 */
public class CodeCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStream<Event> streamSource = env.fromCollection(events);
        streamSource.print();
        fromEle(env);
        fromFile(env);
        env.execute();
    }

    public static void fromEle(StreamExecutionEnvironment env) {
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        eventDataStreamSource.print();
    }

    public static void fromFile(StreamExecutionEnvironment env) {
        DataStream<String> stream = env.readTextFile("Flink/src/main/resources/clicks.csv");
        stream.print();
    }
}
