package org.example.alvin.flink.datastreamapi;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class MinimumFunctionTest {
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
        items.keyBy(0).maxBy(2).print();

        String jobName = "user defined streaming source";
        env.execute(jobName);

        /*
        execution results:
        ========== max ==========
        3> (0,1,0)
        3> (0,1,1)
        3> (0,1,2)     => incorrect data
        3> (0,1,3)
        3> (1,2,5)
        3> (1,2,9)
        3> (1,2,11)
        3> (1,2,13)
        ========== maxBy ==========
        3> (0,1,0)
        3> (0,1,1)
        3> (0,2,2)
        3> (0,1,3)
        3> (1,2,5)
        3> (1,2,9)
        3> (1,2,11)
        3> (1,2,13)

        PS: max() and maxBy both return the element which has the maximum value at the provided field index. the difference is:
            max() would put the key and maximum field value at the specific field, for other field, doesn't make sure the value is correct

        Note: for all aggregation functions, flink would keep the state for them and don't clean up. so in production, we should avoid using
        aggregations functions at the unlimited data stream
         */
    }
}
