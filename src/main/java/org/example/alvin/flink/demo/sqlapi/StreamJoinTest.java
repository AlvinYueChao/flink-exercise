package org.example.alvin.flink.demo.sqlapi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.example.alvin.flink.demo.util.MyStreamingSource;
import org.example.alvin.flink.demo.util.MyStreamingSource.Item;

import java.util.ArrayList;
import java.util.List;

public class StreamJoinTest {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        SingleOutputStreamOperator<Item> source = bsEnv.addSource(new MyStreamingSource())
                .map((MapFunction<Item, Item>) value -> value);

        // split only support seperate stream once.
        // SideOutput support seperate stream multiple times.
        DataStream<Item> evenStream = source.split(new EvenOddSelector()).select("even");
        DataStream<Item> oddStream = source.split(new EvenOddSelector()).select("odd");

        bsTableEnv.registerDataStream("evenTable", evenStream, "name,id");
        bsTableEnv.registerDataStream("oddTable", oddStream, "name,id");

        Table queryTable = bsTableEnv.sqlQuery("select a.id,a.name,b.id,b.name from evenTable as a join oddTable as b on a.name = b.name");

        queryTable.printSchema();

        bsTableEnv.toRetractStream(queryTable, TypeInformation.of(new TypeHint<Tuple4<Integer,String,Integer,String>>(){})).print();

        bsEnv.execute("streaming sql job");
    }

    private static class EvenOddSelector implements OutputSelector<Item> {

        @Override
        public Iterable<String> select(Item value) {
            List<String> output = new ArrayList<>();
            if (value.getId() % 2 == 0) {
                output.add("even");
            } else {
                output.add("odd");
            }
            return output;
        }
    }
}
