package org.example.alvin.flink.demo.sqlapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;

import java.util.Map;

public class EventTimeAndWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(executionEnvironment, envSettings);

        /*streamTableEnv.connect(new ConnectorDescriptor() {
            @Override
            protected Map<String, String> toConnectorProperties() {
                return null;
            }
        })*/

        streamTableEnv.execute("EventTimeAndWatermarkTestJob");
    }
}
