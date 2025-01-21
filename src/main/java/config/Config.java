package config;

import java.util.ArrayList;

public class Config {
    // địa chỉ các broker
    public static final String KAFKA = "192.168.1.6:9092,192.168.1.6:9093,192.168.1.6:9094";
    private static java.util.Collections Collections;
    // Partition ứng với dịch vụ này, ví dụ ở đây dịch vụ này chỉ nghe và chuyển dữ liệu ở partition 0, các dịch vụ nghe và truyền dữ liệu cùng 1 số liệu partition để dễ quản lý
    public static final ArrayList<Integer> PARTITION = new ArrayList<>(java.util.Collections.singletonList(0));
    // Tên topic
    public static final String QUERY_TOPIC = "query";
    public static final String TRAINING_DATA_TOPIC = "training-data";
    public static final String GROUP_ID = "flink group";
    public static final String BROKER = "192.168.1.6:9092,192.168.1.6:9093,192.168.1.6:9094";
}
