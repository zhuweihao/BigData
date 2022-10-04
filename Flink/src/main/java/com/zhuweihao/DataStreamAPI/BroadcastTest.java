package com.zhuweihao.DataStreamAPI;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author zhuweihao
 * @Date 2022/10/4 21:08
 * @Description com.zhuweihao.DataStreamAPI
 */
public class BroadcastTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据源，并行度为1
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());
        //经广播后打印输出，并行度为4
        streamSource.broadcast().print("broadcast").setParallelism(4);
        env.execute();
    }
}
