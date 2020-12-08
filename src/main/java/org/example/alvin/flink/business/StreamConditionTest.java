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
                Boolean optionalCondition = ctx.getBroadcastState(conditionStateDescriptor).get(value.getSourceType());
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
                ctx.getBroadcastState(conditionStateDescriptor).put(value.getSourceType(), enableCondition);
            } catch (Exception e) {
                log.error("Caught exception when switching the condition as command");
            }
            if (enableCondition) {
                log.info("Turn on the condition {} for {} data", value.getConditionName(), value.getSourceType());
            } else {
                log.info("Turn off the condition {} for {} data", value.getConditionName(), value.getSourceType());
            }
        }
    }

    @Data
    private static class ConditionEntity implements Serializable {
        private String conditionName;
        private String sourceType;
        private Boolean enableCondition;
    }

    @Data
    private static class RealTimeDataEntity implements Serializable {
        private String sourceType;
        private String messageBody;
    }

    /*
    test conditionEntity json:
    { "conditionName": "removeDuplicateTrace", "sourceType": "Trace", "enableCondition": true }
    { "conditionName": "removeDuplicateTrace", "sourceType": "Trace", "enableCondition": false }

    test readTimeDataEntity json:
    { "sourceType": "Trace", "messageBody": "message 1" }
    { "sourceType": "Trace", "messageBody": "message 2" }
    { "sourceType": "Trace", "messageBody": "message 3" }
    { "sourceType": "Trace", "messageBody": "message 4" }
     */
}
