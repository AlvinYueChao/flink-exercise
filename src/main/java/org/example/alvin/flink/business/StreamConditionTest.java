package org.example.alvin.flink.business;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.time.OffsetDateTime;

@Slf4j
public class StreamConditionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MapStateDescriptor<String, Boolean> conditionStateDescriptor = new MapStateDescriptor<>("conditionStateDescriptor", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.BOOLEAN_TYPE_INFO);
        BroadcastStream<ConditionEntity> conditionEntityStream = env.socketTextStream("192.168.20.11", 9000).map((MapFunction<String, ConditionEntity>) value -> {
            ObjectMapper objectMapper = new ObjectMapper();
            ConditionEntity conditionEntity;
            try {
                conditionEntity = objectMapper.readValue(value, ConditionEntity.class);
            } catch (JsonProcessingException e) {
                conditionEntity = new ConditionEntity();
            }
            return conditionEntity;
        }).broadcast(conditionStateDescriptor);

        SingleOutputStreamOperator<RealTimeDataEntity> realTimeDataStream = env.socketTextStream("192.168.20.11", 9001).map((MapFunction<String, RealTimeDataEntity>) value -> {
            ObjectMapper objectMapper = new ObjectMapper();
            RealTimeDataEntity realTimeDataEntity;
            try {
                realTimeDataEntity = objectMapper.readValue(value, RealTimeDataEntity.class);
            } catch (JsonProcessingException e) {
                realTimeDataEntity = new RealTimeDataEntity();
            }
            return realTimeDataEntity;
        });

        SingleOutputStreamOperator<RealTimeDataEntity> conditionalRealTimeDataStream = realTimeDataStream.connect(conditionEntityStream).process(new MyProcessFunction());

        conditionalRealTimeDataStream.print("enabled condition stream");
        conditionalRealTimeDataStream.getSideOutput(MyProcessFunction.CONDITION_DISABLED_TAG).print("disabled condition stream");

        env.execute();
    }

    private static class MyProcessFunction extends BroadcastProcessFunction<RealTimeDataEntity, ConditionEntity, RealTimeDataEntity> {
        public static final OutputTag<RealTimeDataEntity> CONDITION_DISABLED_TAG = new OutputTag<RealTimeDataEntity>("conditionDisabledTag"){};
        private final MapStateDescriptor<String, Boolean> conditionStateDescriptor = new MapStateDescriptor<>("conditionStateDescriptor", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.BOOLEAN_TYPE_INFO);

        @Override
        public void processElement(RealTimeDataEntity value, ReadOnlyContext ctx, Collector<RealTimeDataEntity> out) {
            boolean enableCondition = true;
            try {
                Boolean optionalCondition = ctx.getBroadcastState(conditionStateDescriptor).get(value.getConditionName());
                enableCondition = BooleanUtils.isNotFalse(optionalCondition);
            } catch (Exception e) {
                log.error("Caught exception when getting last condition, so default turn on the condition");
            }
            if (enableCondition) {
                out.collect(value);
                log.info("received message {} and enabled condition process logic", value);
            } else {
                ctx.output(CONDITION_DISABLED_TAG, value);
                log.info("received message {} and disabled condition process logic", value);
            }
        }

        @Override
        public void processBroadcastElement(ConditionEntity value, Context ctx, Collector<RealTimeDataEntity> out) {
            Boolean enableCondition = value.getEnableCondition();
            try {
                ctx.getBroadcastState(conditionStateDescriptor).put(value.getConditionName(), enableCondition);
            } catch (Exception e) {
                log.error("Caught exception when switching the condition as command");
            }
            if (enableCondition) {
                log.info("Turn on the condition {} at {}", value.getConditionName(), OffsetDateTime.now());
            } else {
                log.info("Turn off the condition {} at {}", value.getConditionName(), OffsetDateTime.now());
            }
        }
    }

    @Data
    private static class ConditionEntity implements Serializable {
        /**
         * conditionName format: {functionality}-{sourceType}-{productType}
         */
        private String conditionName;
        private Boolean enableCondition;
    }

    @Data
    private static class RealTimeDataEntity implements Serializable {
        private String conditionName;
        private String messageBody;
    }

    /*
    test conditionEntity json:
    { "conditionName": "RemoveDuplicate-Trace-MBS", "enableCondition": true }
    { "conditionName": "RemoveDuplicate-Trace-MBS", "enableCondition": false }

    test readTimeDataEntity json:
    { "conditionName": "RemoveDuplicate-Trace-MBS", "messageBody": "message 1" }
    { "conditionName": "RemoveDuplicate-Trace-MBS", "messageBody": "message 2" }
    { "conditionName": "RemoveDuplicate-Trace-MBS", "messageBody": "message 3" }
    { "conditionName": "RemoveDuplicate-Trace-MBS", "messageBody": "message 4" }
     */

    /*
    demo result:
    enabled condition stream:2> StreamConditionTest.RealTimeDataEntity(conditionName=RemoveDuplicate-Trace-MBS, messageBody=message 1)
    2020-12-09 08:43:51,012  INFO StreamConditionTest:72 - received message StreamConditionTest.RealTimeDataEntity(conditionName=RemoveDuplicate-Trace-MBS, messageBody=message 1) and enabled condition process logic
    2020-12-09 08:44:04,553  INFO StreamConditionTest:90 - Turn off the condition RemoveDuplicate-Trace-MBS at 2020-12-09T08:44:04.553+08:00
    2020-12-09 08:44:04,553  INFO StreamConditionTest:90 - Turn off the condition RemoveDuplicate-Trace-MBS at 2020-12-09T08:44:04.553+08:00
    2020-12-09 08:44:04,553  INFO StreamConditionTest:90 - Turn off the condition RemoveDuplicate-Trace-MBS at 2020-12-09T08:44:04.553+08:00
    2020-12-09 08:44:04,553  INFO StreamConditionTest:90 - Turn off the condition RemoveDuplicate-Trace-MBS at 2020-12-09T08:44:04.553+08:00
    disabled condition stream:3> StreamConditionTest.RealTimeDataEntity(conditionName=RemoveDuplicate-Trace-MBS, messageBody=message 2)
    2020-12-09 08:44:15,160  INFO StreamConditionTest:75 - received message StreamConditionTest.RealTimeDataEntity(conditionName=RemoveDuplicate-Trace-MBS, messageBody=message 2) and disabled condition process logic
    disabled condition stream:4> StreamConditionTest.RealTimeDataEntity(conditionName=RemoveDuplicate-Trace-MBS, messageBody=message 3)
    2020-12-09 08:44:22,412  INFO StreamConditionTest:75 - received message StreamConditionTest.RealTimeDataEntity(conditionName=RemoveDuplicate-Trace-MBS, messageBody=message 3) and disabled condition process logic
    2020-12-09 08:44:34,174  INFO StreamConditionTest:88 - Turn on the condition RemoveDuplicate-Trace-MBS at 2020-12-09T08:44:34.174+08:00
    2020-12-09 08:44:34,174  INFO StreamConditionTest:88 - Turn on the condition RemoveDuplicate-Trace-MBS at 2020-12-09T08:44:34.174+08:00
    2020-12-09 08:44:34,174  INFO StreamConditionTest:88 - Turn on the condition RemoveDuplicate-Trace-MBS at 2020-12-09T08:44:34.174+08:00
    2020-12-09 08:44:34,174  INFO StreamConditionTest:88 - Turn on the condition RemoveDuplicate-Trace-MBS at 2020-12-09T08:44:34.174+08:00
    enabled condition stream:1> StreamConditionTest.RealTimeDataEntity(conditionName=RemoveDuplicate-Trace-MBS, messageBody=message 4)
    2020-12-09 08:44:41,766  INFO StreamConditionTest:72 - received message StreamConditionTest.RealTimeDataEntity(conditionName=RemoveDuplicate-Trace-MBS, messageBody=message 4) and enabled condition process logic
     */
}
