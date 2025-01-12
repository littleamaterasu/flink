package config;

import java.util.ArrayList;

public class Config {
    public static final String KAFKA = "192.168.1.6:9092,192.168.1.6:9093,192.168.1.6:9094";
    private static java.util.Collections Collections;
    public static final ArrayList<Integer> PARTITION = new ArrayList<>(java.util.Collections.singletonList(0));
    public static final String QUERY_TOPIC = "query";
    public static final String TRAINING_DATA_TOPIC = "training-data";
    public static final String GROUP_ID = "flink group";
    public static final String BROKER = "192.168.1.6:9092,192.168.1.6:9093,192.168.1.6:9094";
}
