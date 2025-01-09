package customClass;

import config.Config;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

// Lớp chính
public class FlinkML {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkML.class);
    private static final MapStateDescriptor<String, INB> mapStateDescriptor = new MapStateDescriptor<>(
            "INBBroadcastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<INB>() {
            })
    );

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO: get checkpoint from elasticsearch or redis

        // Luồng kafka training data
        DataStream<String> kafkaStreamTrainingData = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers(Config.KAFKA)
                        .setTopics(Config.TRAINING_DATA_TOPIC)
                        .setGroupId(Config.GROUP_ID)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source Training Data"
        );

        // Luồng kafka query
        DataStream<String> kafkaStreamQuery = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers(Config.KAFKA)
                        .setTopics(Config.QUERY_TOPIC)
                        .setGroupId(Config.GROUP_ID)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source customClass.Query"
        );

        // Tạo Broadcast State Descriptor để mô tả cấu trúc của Query
        MapStateDescriptor<String, INB> inbBroadcast = new MapStateDescriptor<>(
                "INBBroadcast", // Tên của Broadcast State
                String.class,          // Key type
                INB.class            // Value type
        );

        // Luồng data ánh xạ sang customClass.CustomINBData để mô hình sử dụng
        DataStream<CustomINBData> trainingStream = kafkaStreamTrainingData.map(json -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(json, CustomINBData.class);
        }).returns(TypeInformation.of(new TypeHint<CustomINBData>() {
        }));

        // Luồng data ánh xạ sang customClass.Query để thực hiện phân lớp truy vấn
        DataStream<Query> queryDataStream = kafkaStreamQuery.map(json -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(json, Query.class);
        }).returns(TypeInformation.of(new TypeHint<Query>() {
        }));

        // Đảm bảo kiểu dữ liệu khớp với kiểu định nghĩa của BroadcastState và MapState
        DataStream<INB> processedStream = queryDataStream
                .connect(trainingStream.broadcast(inbBroadcast)) // Kết nối với training broadcast
                .process(new BroadcastProcessFunction<Query, CustomINBData, INB>() {
                    private INB inb = new INB();

                    @Override
                    public void processElement(Query query, ReadOnlyContext ctx, Collector<INB> out) {
                        // Truy cập BroadcastState
                        final ReadOnlyBroadcastState<String, INB> inbBroadcastState = ctx.getBroadcastState(inbBroadcast);
                        try {
                            // Lấy trạng thái broadcast từ BroadcastState
                            final INB broadcastINB = inbBroadcastState.get(inb.getName());
                            if (broadcastINB != null) {
                                // Xử lý logic nếu tìm thấy INB trong broadcast
                                out.collect(broadcastINB);
                                broadcastINB.info();

                                // (modified to get array list of label)
                                ArrayList<String> topIndex = inb.predict(query);

                                // TODO: create class response with timestamp, uid, array list of string (topIndex)

                                // TODO: send to kafka with topic "query-res"
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Error when processing element", e);
                        }
                    }

                    @Override
                    public void processBroadcastElement(CustomINBData customINBData, Context ctx, Collector<INB> out) {
                        // Cập nhật INB từ broadcast element
                        inb.update(customINBData);

                        // Truy cập BroadcastState để cập nhật
                        BroadcastState<String, INB> broadcastState = ctx.getBroadcastState(inbBroadcast);

                        try {
                            // Cập nhật trạng thái broadcast với INB mới
                            broadcastState.put(inb.getName(), inb);
                        } catch (Exception e) {
                            System.out.println("Exception when updating INB model");
                            throw new RuntimeException("Error when updating BroadcastState", e);
                        }

                        // TODO: save checkpoint to elasticsearch or redis
                    }
                });


        // Xử lý dữ liệu sau khi đã nhận broadcast query và training data
        processedStream.print();

        // Chạy Flink job
        env.execute("Kafka Stream Processing with Broadcast");
    }

}
