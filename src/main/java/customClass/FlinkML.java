package customClass;

import config.Config;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;

// Lớp chính
public class FlinkML {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkML.class);
    private static final MapStateDescriptor<String, INB> mapStateDescriptor = new MapStateDescriptor<>(
            "INBBroadcastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<INB>() {
            })
    );
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        DataStream<CustomINBData> trainingStream = kafkaStreamTrainingData.map(json -> mapper.readValue(json, CustomINBData.class))
                .returns(TypeInformation.of(CustomINBData.class));

        // Luồng data ánh xạ sang customClass.Query để thực hiện phân lớp truy vấn
        DataStream<Query> queryDataStream = kafkaStreamQuery.map(json -> mapper.readValue(json, Query.class))
                .returns(TypeInformation.of(Query.class));


        // Đảm bảo kiểu dữ liệu khớp với kiểu định nghĩa của BroadcastState và MapState
        DataStream<String> processedStream = queryDataStream
                .connect(trainingStream.broadcast(inbBroadcast)) // Kết nối với training broadcast
                .process(new BroadcastProcessFunction<Query, CustomINBData, String>() {
                    private INB inb = new INB();

                    @Override
                    public void processElement(Query query, ReadOnlyContext ctx, Collector<String> out) {
                        // Truy cập BroadcastState
                        final ReadOnlyBroadcastState<String, INB> inbBroadcastState = ctx.getBroadcastState(inbBroadcast);
                        try {
                            // Lấy trạng thái broadcast từ BroadcastState
                            final INB broadcastINB = inbBroadcastState.get(inb.getName());
                            if (broadcastINB != null) {
                                // Xử lý logic nếu tìm thấy INB trong broadcast
                                broadcastINB.info();

                                // (modified to get array list of label)
                                ArrayList<String> topIndex = inb.predict(query);

                                // response
                                Response response = new Response(Instant.now().toEpochMilli(), query.uid, topIndex);
                                String responseJson = response.toString();

                                // Sử dụng collector để gửi dữ liệu tới Kafka
                                out.collect(responseJson);  // Gửi responseJson vào output stream
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Error when processing element", e);
                        }
                    }

                    @Override
                    public void processBroadcastElement(CustomINBData customINBData, Context ctx, Collector<String> out) {
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
                    }
                });

        // brokers
        String kafkaBootstrapServers = Config.BROKER;

        // topic
        String topic = "classification";

        // KafkaSink configuration
        KafkaSink<String> kafkaSink = createKafkaSink(kafkaBootstrapServers, topic);

        // gửi data
        processedStream.sinkTo(kafkaSink);

        // Xử lý dữ liệu sau khi đã nhận broadcast query và training data
        processedStream.print();

        // Chạy Flink job
        env.execute("Kafka Stream Processing with Broadcast");
    }

    private static KafkaSink<String> createKafkaSink(String kafkaBootstrapServers, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
