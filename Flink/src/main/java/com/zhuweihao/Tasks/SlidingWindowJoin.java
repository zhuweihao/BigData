package com.zhuweihao.Tasks;

import com.alibaba.fastjson.JSON;
import com.zhuweihao.POJO.userbehavior;
import com.zhuweihao.Utils.JSONUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author zhuweihao
 * @Date 2022/11/30 15:42
 * @Description com.zhuweihao.Tasks
 */
public class SlidingWindowJoin {

    private static OutputTag<userbehavior> pv = new OutputTag<userbehavior>("pv") {
    };
    private static OutputTag<userbehavior> buy = new OutputTag<userbehavior>("buy") {
    };
    private static OutputTag<userbehavior> cart = new OutputTag<userbehavior>("cart") {
    };
    private static OutputTag<userbehavior> fav = new OutputTag<userbehavior>("fav") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(4);

        KafkaSource<String> userBehavior_source = KafkaSource.<String>builder()
                .setBootstrapServers("172.22.5.12:9092")
                .setGroupId("zwh")
                .setTopics("userbehavior_01-cdc")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        SingleOutputStreamOperator<userbehavior> process = executionEnvironment
                .fromSource(userBehavior_source, WatermarkStrategy.forMonotonousTimestamps(), "userbehavior")
                .process(new ProcessFunction<String, userbehavior>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, userbehavior>.Context context, Collector<userbehavior> collector) throws Exception {
                        userbehavior after = JSONUtil.jsonToPojo(JSON.parseObject(s).getString("after"), userbehavior.class);
                        String behavior_type = after.getBehavior_type();
                        if (behavior_type.equalsIgnoreCase("pv")) {
                            context.output(pv, after);
                        } else if (behavior_type.equalsIgnoreCase("buy")) {
                            context.output(buy, after);
                        } else if (behavior_type.equalsIgnoreCase("cart")) {
                            context.output(cart, after);
                        } else if (behavior_type.equalsIgnoreCase("fav")) {
                            context.output(fav, after);
                        }
                    }
                });
        DataStream<userbehavior> PV = process.getSideOutput(pv);
        DataStream<userbehavior> BUY = process.getSideOutput(buy);
        DataStream<userbehavior> FAV = process.getSideOutput(fav);


        FAV
                .join(BUY)
                .where(r->r.getItem_id())
                .equalTo(r->r.getItem_id())
                .window(SlidingEventTimeWindows.of(Time.seconds(20),Time.seconds(5)))
                .apply(new JoinFunction<userbehavior, userbehavior, userbehavior>() {
                    @Override
                    public userbehavior join(userbehavior userbehavior, userbehavior userbehavior2) throws Exception {
                        return userbehavior;
                    }
                })
                .print();
        executionEnvironment.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<userbehavior> {

        @Override
        public WatermarkGenerator<userbehavior> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }

        @Override
        public TimestampAssigner<userbehavior> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<userbehavior>() {
                @Override
                public long extractTimestamp(userbehavior userbehavior, long l) {
                    return (long) userbehavior.getTimestamp();
                }
            };
        }
    }

    public static class CustomPeriodicGenerator implements WatermarkGenerator<userbehavior> {

        //延迟时间
        private final Long delayTime = 5000L;
        //观察到的最大时间戳
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L;

        @Override
        public void onEvent(userbehavior userbehavior, long l, WatermarkOutput watermarkOutput) {
            //每来一条数据就调用一次,更新最大时间戳
            maxTs = Math.max((long) userbehavior.getTimestamp(), maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            //发射水位线，默认200ms调用一次
            watermarkOutput.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }


}
