package org.example.alvin.flink.demo.datastreamapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ReduceFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<Integer, Integer, Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>(0, 1, 0));
        data.add(new Tuple3<>(0, 1, 1));
        data.add(new Tuple3<>(0, 2, 2));
        data.add(new Tuple3<>(0, 1, 3));
        data.add(new Tuple3<>(1, 2, 5));
        data.add(new Tuple3<>(1, 2, 9));
        data.add(new Tuple3<>(1, 2, 11));
        data.add(new Tuple3<>(1, 2, 13));

        DataStreamSource<Tuple3<Integer, Integer, Integer>> items = env.fromCollection(data);

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> reduce = items.keyBy(0).reduce((ReduceFunction<Tuple3<Integer, Integer, Integer>>) (t1, t2) -> {
            Tuple3<Integer, Integer, Integer> newTuple = new Tuple3<>();

            newTuple.setFields(0, 0, (Integer) t1.getField(2) + (Integer) t2.getField(2));
            log.info("newTuple: {}", newTuple.toString());
            return newTuple;
        });

        reduce.print("reduce result");

        env.execute();

        /*
        execution results:
        reduce result:3> (0,1,0)
        2020-10-11 00:23:43,998  INFO ReduceFunctionTest:34 - newTuple: (0,0,1)
        reduce result:3> (0,0,1)
        2020-10-11 00:23:43,999  INFO ReduceFunctionTest:34 - newTuple: (0,0,3)
        reduce result:3> (0,0,3)
        2020-10-11 00:23:44,003  INFO ReduceFunctionTest:34 - newTuple: (0,0,6)
        reduce result:3> (0,0,6)

        reduce result:3> (1,2,5)
        2020-10-11 00:23:44,003  INFO ReduceFunctionTest:34 - newTuple: (0,0,14)
        reduce result:3> (0,0,14)
        2020-10-11 00:23:44,005  INFO ReduceFunctionTest:34 - newTuple: (0,0,25)
        reduce result:3> (0,0,25)
        2020-10-11 00:23:44,005  INFO ReduceFunctionTest:34 - newTuple: (0,0,38)
        reduce result:3> (0,0,38)

        PS：The reduce function is consecutively applied to all values of a group until only a single value remains.
         */
    }
}
