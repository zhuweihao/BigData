package com.zhuweihao.TimeAndWindow;

import com.zhuweihao.DataStreamAPI.ClickSource;
import com.zhuweihao.DataStreamAPI.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author zhuweihao
 * @Date 2022/10/10 21:39
 * @Description com.zhuweihao.DataStreamAPI
 */
public class CustomWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .print();
        env.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long l) {
                    //告诉程序数据源里的时间戳是哪一个字段
                    return event.timestamp;
                }
            };
        }
    }

    public static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {

        //延迟时间
        private Long delayTime = 5000L;
        //观察到的最大时间戳
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L;

        @Override
        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
            //每来一条数据就调用一次,更新最大时间戳
            maxTs = Math.max(event.timestamp, maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            //发射水位线，默认200ms调用一次
            watermarkOutput.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }
}
