package com.zhuweihao.MultiStreamConversion;

import com.zhuweihao.DataStreamAPI.ClickSource;
import com.zhuweihao.DataStreamAPI.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author zhuweihao
 * @Date 2022/10/19 15:55
 * @Description com.zhuweihao.MultiStreamConversion
 */
public class SplitStreamByOutputTag {
    //定义输出标签，侧输出流的数据类型为三元组（user，url，timestamp）
    private static OutputTag<Tuple3<String, String, Long>> MaryTag = new OutputTag<Tuple3<String, String, Long>>("Mary-pv") {
    };
    private static OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<Tuple3<String, String, Long>>("Bob-pv") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> processStream = stream
                .process(new ProcessFunction<Event, Event>() {
                    @Override
                    public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                        if (value.user.equals("Mary")) {
                            ctx.output(MaryTag, new Tuple3<>(value.user, value.url, value.timestamp));
                        } else if (value.user.equals("Bob")) {
                            ctx.output(BobTag, new Tuple3<>(value.user, value.url, value.timestamp));
                        } else {
                            out.collect(value);
                        }
                    }
                });
        processStream.getSideOutput(MaryTag).print("Mary pv");
        processStream.getSideOutput(BobTag).print("Bob pv");
        processStream.print("else");
        env.execute();
    }
}
