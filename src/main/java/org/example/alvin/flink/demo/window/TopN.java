package org.example.alvin.flink.demo.window;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Comparator;
import java.util.TreeMap;

public class TopN {
    public static void main(String[] args) {
        TreeMap<Double, OrderDetail> treeMap = new TreeMap<Double, OrderDetail>(new Comparator<Double>() {
            @Override
            public int compare(Double x, Double y) {
                return Integer.compare(new BigDecimal(x).compareTo(new BigDecimal(y)), 0);
            }
        });

        OrderDetail[] orders = new OrderDetail[] {
                new OrderDetail(1L, 2L, "ShangHai", 100.0, Instant.now().toEpochMilli()),
                new OrderDetail(1L, 3L, "ShangHai", 20.0, Instant.now().toEpochMilli() + 100),
                new OrderDetail(1L, 4L, "ShangHai", 30.0, Instant.now().toEpochMilli() + 200),
                new OrderDetail(1L, 5L, "ShangHai", 10.0, Instant.now().toEpochMilli() + 300),
        };
        for (OrderDetail order : orders) {
            treeMap.put(order.getPrice(), order);
            if (treeMap.size() > 3) {
                treeMap.pollLastEntry();
            }
        }
    }
}
