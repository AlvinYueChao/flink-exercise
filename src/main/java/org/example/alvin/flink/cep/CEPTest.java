package org.example.alvin.flink.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;

@Slf4j
public class CEPTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> eventStrStream = env.socketTextStream("192.168.20.11", 9000);

        SingleOutputStreamOperator<Event> input = eventStrStream.map((MapFunction<String, Event>) eventStr -> {
            String[] eventParts = eventStr.split(",");
            Event event;
            if (eventParts.length == 3) {
                event = new Event(Integer.parseInt(eventParts[0]), new BigDecimal(eventParts[1]), eventParts[2]);
            } else {
                event = new Event();
            }
            return event;
        });


    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Event {
        private Integer id;
        private BigDecimal volume;
        private String name;
    }
}
