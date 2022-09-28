package com.zhuweihao.DataStreamAPI;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author zhuweihao
 * @Date 2022/9/28 15:22
 * @Description com.zhuweihao.DataStreamAPI
 */
public class TransTupleAggreation {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> streamSource = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamSource.keyBy(r -> r.f0);
        keyedStream.sum(1).print("sum1");
        keyedStream.sum("f1").print("sum2");
        keyedStream.max(1).print("max1");
        keyedStream.max("f1").print("max2");
        keyedStream.maxBy(1).print("maxby");
        keyedStream.minBy("f1").print("minby");
        env.execute();
    }
}
