package org.example.alvin.flink.business;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

@Slf4j
public class DistinctEventJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameters = ParameterTool.fromPropertiesFile(DistinctEventJob.class.getResourceAsStream("/application.properties"));

        String topic = parameters.get("source.kafka.topic");

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", parameters.get("source.kafka.bootstrap.servers"));
        kafkaProperties.put("key.deserializer", parameters.get("source.kafka.key.deserializer"));
        kafkaProperties.put("value.deserizliaer", parameters.get("source.kafka.value.deserializer"));
        kafkaProperties.put("group.id", parameters.get("source.kafka.group"));

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), kafkaProperties);

        env.execute();
    }
}
