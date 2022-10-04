package com.zhuweihao.DataStreamAPI;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author zhuweihao
 * @Date 2022/10/4 12:58
 * @Description com.zhuweihao.DataStreamAPI
 */
public class ShuffleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据源，并行度为1
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());
        //经洗牌后打印输出，并行度为4
        streamSource.shuffle().print("shuffle").setParallelism(4);
        env.execute();
    }
}
