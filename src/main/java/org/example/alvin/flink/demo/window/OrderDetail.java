package org.example.alvin.flink.demo.window;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OrderDetail {
    private Long userId;
    private Long itemId;
    private String cityName;
    private Double price;
    private Long timeStamp;
}