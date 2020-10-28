package org.example.alvin.flink.demo.util;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MyStreamingSource implements SourceFunction<MyStreamingSource.Item> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while (isRunning) {
            Item item = generateItem();
            ctx.collect(item);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private Item generateItem() {
        int i = new Random().nextInt(100);

        List<String> list = new ArrayList<>();
        list.add("HAT");
        list.add("TIE");
        list.add("SHOE");

        Item item = new Item();
        item.setName(list.get(new Random().nextInt(3)));
        item.setId(i);
        return item;
    }

    @NoArgsConstructor
    @Data
    public static class Item {
        private String name;
        private Integer id;

        @Override
        public String toString() {
            return "Item{" +
                    "name='" + name + '\'' +
                    ", id=" + id +
                    '}';
        }
    }
}
