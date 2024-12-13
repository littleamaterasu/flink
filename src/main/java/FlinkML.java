import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;

// Lớp chính
public class FlinkML {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkML.class);

    // Broadcast State Descriptor
    private static final MapStateDescriptor<String, IncrementalNaiveBayesModel> MODEL_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("ModelBroadcastState", String.class, IncrementalNaiveBayesModel.class);

    public static void main(String[] args) throws Exception {
        // Thiết lập môi trường Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Stream nguồn từ Kafka
        DataStream<String> kafkaStream = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers("kafka:29092")
                        .setTopics("training-data")
                        .setGroupId("flink-group")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Parse JSON từ Kafka
        DataStream<CustomData> trainingStream = kafkaStream.map(json -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(json, CustomData.class);
        }).returns(TypeInformation.of(new TypeHint<CustomData>() {
        }));

        // Tạo BroadcastStream từ trainingStream
        BroadcastStream<IncrementalNaiveBayesModel> broadcastStream = trainingStream
                .keyBy(data -> "globalModel") // Phân nhóm toàn bộ dữ liệu vào cùng một key
                .map(new RichMapFunction<CustomData, IncrementalNaiveBayesModel>() {
                    private transient ValueState<IncrementalNaiveBayesModel> modelState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<IncrementalNaiveBayesModel> descriptor =
                                new ValueStateDescriptor<>("naiveBayesModel", IncrementalNaiveBayesModel.class);
                        modelState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public IncrementalNaiveBayesModel map(CustomData value) throws Exception {
                        IncrementalNaiveBayesModel model = modelState.value();
                        if (model == null) {
                            model = new IncrementalNaiveBayesModel();
                        }

                        // Cập nhật mô hình với dữ liệu mới
                        Vector features = new DenseVector(tokensToFeatures(value.tokens));
                        for (String label : extractKeywords(value.keywords)) {
                            double labelId = model.getOrCreateLabelId(label);
                            model.update(features, labelId);
                        }

                        // Lưu lại mô hình đã cập nhật
                        modelState.update(model);

                        LOG.info("Model updated with new training data: {}", model);
                        return model;
                    }
                })
                .broadcast(MODEL_STATE_DESCRIPTOR); // Tiếp tục truyền đi qua BroadcastStream


        // Stream đầu vào từ socket cho dự đoán
        DataStream<String> predictionStream = env.socketTextStream("host.docker.internal", 9999);

        // Kết nối predictionStream với broadcastStream
        BroadcastConnectedStream<String, IncrementalNaiveBayesModel> connectedStream =
                predictionStream.connect(broadcastStream);

        // Xử lý dự đoán
        connectedStream.process(new BroadcastProcessFunction<String, IncrementalNaiveBayesModel, String>() {
            @Override
            public void processElement(String tokensInput, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                String[] tokens = tokensInput.split(",");
                Vector features = new DenseVector(tokensToFeatures(tokens));

                // Lấy mô hình từ Broadcast State
                IncrementalNaiveBayesModel broadcastModel = ctx.getBroadcastState(MODEL_STATE_DESCRIPTOR).get("currentModel");
                if (broadcastModel == null) {
                    LOG.warn("No model available for prediction!");
                    out.collect("No model available for prediction!");
                    return;
                }

                List<Double> prediction = broadcastModel.predictTop5(features);
                LOG.info("Real-time prediction by " + broadcastModel.classCounts.size());
                for (Double label : prediction) {
                    String labelName = broadcastModel.getLabelName(label);
                    LOG.info("Real-time Prediction: " + labelName + " for tokens: [" + String.join(", ", tokens) + "]");
                    out.collect("Real-time Prediction: " + labelName + " for tokens: [" + String.join(", ", tokens) + "]");
                }
            }

            @Override
            public void processBroadcastElement(IncrementalNaiveBayesModel updatedModel, Context ctx, Collector<String> out) throws Exception {
                // Lưu mô hình mới vào trạng thái broadcast
                ctx.getBroadcastState(MODEL_STATE_DESCRIPTOR).put("currentModel", updatedModel);

                // Kiểm tra xem lấy model có đúng không
                LOG.info("predicting model has " + updatedModel.classCounts.size());
            }
        }).print();

        // Chạy Flink pipeline
        env.execute("Flink Naive Bayes with Broadcast Model");
    }

    // Hàm chuyển đổi tokens thành vector đặc trưng
    public static double[] tokensToFeatures(String[] tokens) {
        Map<String, Integer> tokenMap = new HashMap<>();
        for (String token : tokens) {
            tokenMap.put(token, tokenMap.getOrDefault(token, 0) + 1);
        }
        int maxFeatureSize = 400; // Đảm bảo kích thước vector cố định
        double[] features = new double[maxFeatureSize];
        int index = 0;
        for (Integer count : tokenMap.values()) {
            if (index >= maxFeatureSize) break;
            features[index++] = count;
        }
        return features;
    }

    // Hàm tách trường keywords thành danh sách nhãn
    public static List<String> extractKeywords(String keywords) {
        return Arrays.asList(keywords.split(","));
    }

    // Lớp đại diện cho mô hình Naive Bayes cập nhật theo thời gian thực
    public static class IncrementalNaiveBayesModel implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private final Map<Double, double[]> classFeatureSums = new HashMap<>();
        private final Map<Double, Integer> classCounts = new HashMap<>();
        private final Map<String, Double> labelToId = new HashMap<>();
        private final Map<Double, String> idToLabel = new HashMap<>();
        private double nextLabelId = 0.0;

        public synchronized double getOrCreateLabelId(String label) {
            return labelToId.computeIfAbsent(label, k -> {
                double id = nextLabelId++;
                idToLabel.put(id, k);
                return id;
            });
        }

        public synchronized String getLabelName(double labelId) {
            return idToLabel.getOrDefault(labelId, "Unknown");
        }

        public void update(Vector features, double label) {
            double[] featureArray = features.toArray();
            classFeatureSums.putIfAbsent(label, new double[featureArray.length]);
            classCounts.put(label, classCounts.getOrDefault(label, 0) + 1);

            double[] featureSums = classFeatureSums.get(label);
            if (featureSums.length != featureArray.length) {
                throw new IllegalStateException("Feature vector size mismatch: expected "
                        + featureSums.length + ", got " + featureArray.length);
            }

            for (int i = 0; i < featureArray.length; i++) {
                featureSums[i] += featureArray[i];
            }
        }

        public List<Double> predictTop5(Vector features) {
            double[] featureArray = features.toArray();

            // PriorityQueue để lưu top 5 nhãn với điểm số cao nhất (giảm dần)
            PriorityQueue<Map.Entry<Double, Double>> topLabels = new PriorityQueue<>(
                    Comparator.comparingDouble(Map.Entry::getValue) // Sắp xếp theo điểm số tăng dần
            );

            for (Map.Entry<Double, double[]> entry : classFeatureSums.entrySet()) {
                double label = entry.getKey();
                double[] featureSums = entry.getValue();
                int count = classCounts.get(label);

                double score = 0.0;
                for (int i = 0; i < Math.min(featureArray.length, featureSums.length); i++) {
                    double mean = featureSums[i] / count;
                    score += featureArray[i] * Math.log(mean + 1e-6); // Tránh log(0)
                }

                // Thêm nhãn và điểm số vào PriorityQueue
                topLabels.offer(new AbstractMap.SimpleEntry<>(label, score));

                // Giữ tối đa 5 phần tử trong hàng đợi
                if (topLabels.size() > 5) {
                    topLabels.poll(); // Loại bỏ phần tử nhỏ nhất
                }
            }

            // Chuyển PriorityQueue thành danh sách nhãn (giảm dần theo điểm số)
            List<Double> top5Labels = new ArrayList<>();
            while (!topLabels.isEmpty()) {
                top5Labels.add(0, topLabels.poll().getKey()); // Chèn nhãn vào đầu danh sách
            }

            return top5Labels;
        }

    }

    // Lớp đại diện cho cấu trúc JSON
    public static class CustomData {
        public String keywords;
        public String[] tokens;

        @Override
        public String toString() {
            return "CustomData{" +
                    "keywords='" + keywords + '\'' +
                    ", tokens=" + Arrays.toString(tokens) +
                    '}';
        }
    }

    public class INB implements Serializable {
        @Serial
        private static final long serialVersionUID = 2L;

        // mảng số hóa từ vựng
        private Map<String, Integer> dictionary = new HashMap<>();
        // mảng số hóa label
        private final Map<String, Integer> labels = new HashMap<>();
        // mảng 2 chiều để đếm số lần xuất hiện của 1 token trong 1 label
        private final ArrayList<ArrayList<Integer>> tokenPerLabel = new ArrayList<>();
        // số lượng từ mỗi label
        private final ArrayList<Integer> totalTokenOfLabel = new ArrayList<>();
        // số lượng văn bản mỗi label
        private final ArrayList<Integer> totalDocOfLabel = new ArrayList<>();
        private int vocabularySize = 0;
        private int labelsSize = 0;
        private int docCount = 0;

        // Cập nhật mô hình với dữ liệu mới
        public void update(CustomINBData data) {
            for (String label : data.keywords) {
                this.addToDictionary(label);
                this.addLabel(label);
                int index = this.labels.get(label);
                this.totalDocOfLabel.set(index, this.totalDocOfLabel.get(index) + 1);
            }

            for (String token : data.tokens) {
                this.addToDictionary(token);
                for (String label : data.keywords) {
                    int labelIndex = this.labels.get(label);
                    int tokenIndex = this.dictionary.get(token);

                    // Cập nhật số lần xuất hiện của token trong nhãn
                    this.tokenPerLabel.get(labelIndex).set(tokenIndex, this.tokenPerLabel.get(labelIndex).get(tokenIndex) + 1);
                    this.totalTokenOfLabel.set(labelIndex, this.totalTokenOfLabel.get(labelIndex) + 1);
                }
            }

            // Tăng số lượng tài liệu
            ++this.docCount;
        }

        // Dự đoán nhãn cho truy vấn
        public ArrayList<Integer> predict(Query query) {
            Map<String, Integer> queryVocabulary = new HashMap<>();
            for (String token : query.tokens) {
                queryVocabulary.put(token, queryVocabulary.getOrDefault(token, 0) + 1);
            }

            // Tính xác suất cho từng nhãn
            ArrayList<Double> countInLabel = new ArrayList<>(Collections.nCopies(this.labelsSize, 0.0));

            // Cập nhật xác suất của mỗi label (tính xác suất của label dựa trên tần suất xuất hiện trong văn bản)
            for (int i = 0; i < this.labelsSize; ++i) {
                double labelProbability = (double) this.totalDocOfLabel.get(i) / this.docCount;
                countInLabel.set(i, Math.log(labelProbability));
            }

            // Cập nhật xác suất cho mỗi token trong query
            for (Map.Entry<String, Integer> entry : queryVocabulary.entrySet()) {
                String keyword = entry.getKey();
                int count = entry.getValue();

                // Cập nhật xác suất cho từng nhãn
                for (int i = 0; i < this.labelsSize; ++i) {
                    double probability = countInLabel.get(i);
                    if (this.dictionary.containsKey(keyword)) {
                        int tokenIndex = this.dictionary.get(keyword);
                        probability += count * Math.log(
                                (this.tokenPerLabel.get(i).get(tokenIndex) + 1.0) /
                                        (this.totalTokenOfLabel.get(i) + this.vocabularySize)
                        );
                    }
                    countInLabel.set(i, probability);
                }
            }

            // Lấy các nhãn có xác suất cao nhất
            ArrayList<Integer> topIndexes = new ArrayList<>();
            PriorityQueue<int[]> maxHeap = new PriorityQueue<>((a, b) -> Double.compare(b[0], a[0]));

            for (int i = 0; i < countInLabel.size(); i++) {
                maxHeap.add(new int[]{(int) countInLabel.get(i).doubleValue(), i});
            }

            int topCount = Math.min(5, maxHeap.size());
            for (int i = 0; i < topCount; i++) {
                topIndexes.add(Objects.requireNonNull(maxHeap.poll())[1]);
            }

            return topIndexes;
        }

        // Thêm token vào từ điển
        public void addToDictionary(String token) {
            if (!this.dictionary.containsKey(token)) {
                this.dictionary.put(token, this.vocabularySize);

                // Thêm hàng mới vào ma trận tokenPerLabel
                for (ArrayList<Integer> arrayList : this.tokenPerLabel) {
                    arrayList.add(0);
                }
                ++this.vocabularySize;
            }
        }

        // Thêm label vào hệ thống
        public void addLabel(String label) {
            if (!this.labels.containsKey(label)) {
                this.labels.put(label, this.labelsSize);

                // Thêm cột mới vào ma trận tokenPerLabel
                if (!this.tokenPerLabel.isEmpty()) {
                    int size = this.tokenPerLabel.get(0).size();
                    ArrayList<Integer> newRow = new ArrayList<>(Collections.nCopies(size, 0));
                    this.tokenPerLabel.add(newRow);
                } else {
                    this.tokenPerLabel.add(new ArrayList<>());
                }

                // Thêm vào mảng đếm số token của label
                this.totalTokenOfLabel.add(0);
                // Thêm vào 1 label mới đếm số doc
                this.totalDocOfLabel.add(0);
                ++labelsSize;
            }
        }

        public Map<String, Integer> getDictionary() {
            return dictionary;
        }

        public void setDictionary(Map<String, Integer> dictionary) {
            this.dictionary = dictionary;
        }

        // CustomINBData là đối tượng chứa dữ liệu huấn luyện
        public static class CustomINBData {
            public List<String> keywords; // Nhãn (label)
            public List<String> tokens;   // Các token trong văn bản
        }

        // Query là đối tượng chứa dữ liệu truy vấn
        public static class Query {
            public List<String> tokens; // Các token trong truy vấn
        }
    }
}
