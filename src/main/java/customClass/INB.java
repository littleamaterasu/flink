package customClass;

import org.apache.commons.math3.util.Pair;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;

public class INB implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

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
        // thêm label vào danh sách các label
        for (String label : data.keywords) {
            this.addToDictionary(label);

            // thực hiện thêm label
            this.addLabel(label);

            // index của label
            int index = this.labels.get(label);

            // tăng doc count của label
            this.totalDocOfLabel.set(index, this.totalDocOfLabel.get(index) + 1);
        }

        // thêm vào bộ từ vựng của label
        for (String token : data.tokens) {

            // thêm vào từ điển tổng
            this.addToDictionary(token);

            // thêm vào từng label ứng với keyword
            for (String label : data.keywords) {

                // vị trí của label
                int labelIndex = this.labels.get(label);

                // vị trí label trong từ điển
                int tokenIndex = this.dictionary.get(token);

                // Cập nhật số lần xuất hiện của token trong nhãn
                this.tokenPerLabel.get(labelIndex).set(tokenIndex, this.tokenPerLabel.get(labelIndex).get(tokenIndex) + 1);
                this.totalTokenOfLabel.set(labelIndex, this.totalTokenOfLabel.get(labelIndex) + 1);
            }
        }

        // Tăng số lượng tài liệu
        ++this.docCount;

        // In thông tin mô hình
        this.info();
    }

    // Dự đoán nhãn cho truy vấn
    public ArrayList<String> predict(Query query) {
        Map<String, Integer> queryVocabulary = new HashMap<>();
        for (String token : query.tokens) {
            queryVocabulary.put(token, queryVocabulary.getOrDefault(token, 0) + 1);
        }

        // Tính xác suất cho từng nhãn
        ArrayList<Double> countInLabel = new ArrayList<>(Collections.nCopies(this.labelsSize, 0.0));

        // Cập nhật xác suất của mỗi label (tính xác suất của label dựa trên tần suất xuất hiện trong văn bản)
        for (int i = 0; i < this.labelsSize; ++i) {
            double labelProbability = (double) this.totalDocOfLabel.get(i) / this.docCount;
            countInLabel.set(i, labelProbability);
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
        ArrayList<String> topIndexes = new ArrayList<>();
        PriorityQueue<Pair<String, Double>> maxHeap = new PriorityQueue<>(
                (a, b) -> Double.compare(b.getValue(), a.getValue())
        );

        for(Map.Entry<String, Integer> entry : labels.entrySet()){
            maxHeap.add(new Pair<>(entry.getKey(), countInLabel.get(entry.getValue())));
        }

        int topCount = Math.min(5, maxHeap.size());
        for (int i = 0; i < topCount; i++) {
            topIndexes.add(Objects.requireNonNull(maxHeap.poll()).getKey());
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

    public void info() {
        System.out.println("Total doc: " + this.docCount);
        System.out.println("Total label: " + this.labelsSize);
        System.out.println("Total vocab: " + this.vocabularySize);
    }

    public Map<String, Integer> getDictionary() {
        return dictionary;
    }

    public void setDictionary(Map<String, Integer> dictionary) {
        this.dictionary = dictionary;
    }

    public String getName() {
        return "inb";
    }
}