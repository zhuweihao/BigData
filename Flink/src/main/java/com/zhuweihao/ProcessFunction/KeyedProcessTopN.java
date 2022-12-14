package com.zhuweihao.ProcessFunction;

import com.zhuweihao.DataStreamAPI.ClickSource;
import com.zhuweihao.DataStreamAPI.Event;
import com.zhuweihao.TimeAndWindow.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @Author zhuweihao
 * @Date 2022/10/19 14:38
 * @Description com.zhuweihao.ProcessFunction
 */
public class KeyedProcessTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        //??????url?????????????????????url????????????
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = eventStream
                .keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());
        //????????????????????????????????????????????????????????????
        SingleOutputStreamOperator<String> result = urlCountStream
                .keyBy(data -> data.windowEnd)
                .process(new TopN(2));
        result.print("result");
        env.execute();
    }

    //?????????????????????
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    //??????????????????????????????????????????????????????
    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // ???????????????????????????????????????
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(new UrlViewCount(s, elements.iterator().next(), start, end));
        }
    }

    //?????????????????????????????????top n
    public static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {
        //???n????????????
        private Integer n;
        //????????????????????????
        private ListState<UrlViewCount> urlViewCountListState;

        public TopN(Integer n) {
            this.n = n;
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //????????????????????????????????????????????????ArrayList???????????????
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }
            //???????????????????????????
            urlViewCountListState.clear();
            //??????
            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });
            //?????????????????????????????????
            StringBuilder result = new StringBuilder();
            result.append("=================================\n");
            result.append("?????????????????????" + new Timestamp(timestamp - 1) + "\n");
            for (int i = 0; i < this.n; i++) {
                UrlViewCount urlViewCount = urlViewCountArrayList.get(i);
                String info = "No." + (i + 1) + " "
                        + "url???" + urlViewCount.url + " "
                        + "????????????" + urlViewCount.count + "\n";
                result.append(info);
            }
            result.append("=================================\n");
            out.collect(result.toString());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //????????????????????????????????????
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("url-view-count-list", Types.POJO(UrlViewCount.class))
            );
        }

        @Override
        public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            //???count?????????????????????????????????????????????
            urlViewCountListState.add(value);
            //??????window end+1ms??????????????????????????????????????????????????????
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }
    }
}
