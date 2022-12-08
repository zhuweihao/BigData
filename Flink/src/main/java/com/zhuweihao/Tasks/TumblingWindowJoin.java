package com.zhuweihao.Tasks;

import com.alibaba.fastjson.JSON;
import com.zhuweihao.POJO.lineorder;
import com.zhuweihao.Utils.JSONUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Int;

/**
 * @Author zhuweihao
 * @Date 2022/12/7 13:17
 * @Description com.zhuweihao.Tasks
 */
public class TumblingWindowJoin {

    private static OutputTag<lineorder> URGENT = new OutputTag<lineorder>("1") {
    };
    private static OutputTag<lineorder> HIGH = new OutputTag<lineorder>("2") {
    };
    private static OutputTag<lineorder> MEDIUM = new OutputTag<lineorder>("3") {
    };
    private static OutputTag<lineorder> NOT_SPECIFIED = new OutputTag<lineorder>("4") {
    };
    private static OutputTag<lineorder> LOW = new OutputTag<lineorder>("5") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        KafkaSource<String> lineorder_source = KafkaSource.<String>builder()
                .setBootstrapServers("172.22.5.12:9092")
                .setTopics("lineorder-cdc")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<lineorder> process = executionEnvironment
                .fromSource(lineorder_source, WatermarkStrategy.forMonotonousTimestamps(), "lineorder")
                .process(new ProcessFunction<String, lineorder>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, lineorder>.Context context, Collector<lineorder> collector) throws Exception {
                        lineorder lineorder = JSONUtil.jsonToPojo(JSON.parseObject(s).getString("after"), lineorder.class);
                        String lo_orderpriority = null;
                        if (lineorder != null) {
                            lo_orderpriority = lineorder.getLo_orderpriority();
                        }
                        if (lo_orderpriority.equalsIgnoreCase("1-URGENT")) {
                            context.output(URGENT, lineorder);
                        } else if (lo_orderpriority.equalsIgnoreCase("2-HIGH")) {
                            context.output(HIGH, lineorder);
                        } else if (lo_orderpriority.equalsIgnoreCase("3-MEDIUM")) {
                            context.output(MEDIUM, lineorder);
                        } else if (lo_orderpriority.equalsIgnoreCase("4-NOT SPECIFIED")) {
                            context.output(NOT_SPECIFIED, lineorder);
                        } else if (lo_orderpriority.equalsIgnoreCase("5-LOW")) {
                            context.output(LOW, lineorder);
                        }

                    }
                });
        DataStream<lineorder> urgent = process.getSideOutput(URGENT);
        DataStream<lineorder> high = process.getSideOutput(HIGH);
        DataStream<lineorder> medium = process.getSideOutput(MEDIUM);
        DataStream<lineorder> not_specified = process.getSideOutput(NOT_SPECIFIED);
        DataStream<lineorder> low = process.getSideOutput(LOW);

        urgent
                .join(high)
                .where(r -> r.getLo_suppkey())
                .equalTo(r -> r.getLo_suppkey())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<lineorder, lineorder, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> join(lineorder lineorder, lineorder lineorder2) throws Exception {

                        return Tuple3.of(lineorder.getLo_suppkey(), lineorder.getLo_custkey(), lineorder2.getLo_custkey());
                    }
                }).print();

        executionEnvironment.execute();

    }
}
