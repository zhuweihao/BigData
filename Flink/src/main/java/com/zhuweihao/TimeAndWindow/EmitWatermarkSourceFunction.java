package com.zhuweihao.TimeAndWindow;

import com.zhuweihao.DataStreamAPI.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

/**
 * @Author zhuweihao
 * @Date 2022/10/10 21:56
 * @Description com.zhuweihao.DataStreamAPI
 */
public class EmitWatermarkSourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSourceWithWatermark()).print();
        env.execute();
    }

    //泛型是数据源中的类型
    public static class ClickSourceWithWatermark implements SourceFunction<Event> {

        private boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            Random random = new Random();
            String[] userArr = {"Mary", "Bob", "Alice"};
            String[] urlArr = {"./home", "./cart", "./prod?id=1"};
            while (running) {
                //毫秒时间戳
                long currentTs = Calendar.getInstance().getTimeInMillis();
                String username = userArr[random.nextInt(userArr.length)];
                String url = urlArr[random.nextInt(urlArr.length)];
                Event event = new Event(username, url, currentTs);
                //使用collectWithTimestamp方法将数据发送出去，并指明数据中的时间戳的字段
                ctx.collectWithTimestamp(event, event.timestamp);
                //发送水位线
                ctx.emitWatermark(new Watermark(event.timestamp - 1L));
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
