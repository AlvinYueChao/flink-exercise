package org.example.alvin.flink.demo.window;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

@Slf4j
public class EventTimeTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<String> dataStream = env.socketTextStream("192.168.20.11", 9000)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                    private Long currentTimeStamp = 0L;
                    private Long maxOutOfOrderness = 5000L;

                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(this.currentTimeStamp - this.maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(String element, long previousElementTimestamp) {
                        String[] arr = element.split(",");
                        long timestamp = Long.parseLong(arr[1]);
                        this.currentTimeStamp = Math.max(timestamp, currentTimeStamp);
                        log.info("{}, EventTime: {}, watermark: {}", element, timestamp, this.currentTimeStamp - this.maxOutOfOrderness);
                        return timestamp;
                    }
                });

        dataStream.map((MapFunction<String, Tuple2<String, Long>>) value -> {
            String[] arr = value.split(",");
            return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5))).minBy(1)
                .print("Minimum result");

        env.execute("WaterMark Test Demo");

        /*
        PS: EventTime + Window + WaterMark, timer trigger conditions:
        1. watermark >= window_end_time;
        2. [window_start_time, window_end_time), there are element(s) in the window.
         */
    }
}
