package customClass;

import java.util.ArrayList;

// Class Response
public class Response {
    private long timestamp;
    private long uid;
    private ArrayList<String> classes;

    public Response(long timestamp, long uid, ArrayList<String> classes) {
        this.timestamp = timestamp;
        this.uid = uid;
        this.classes = classes;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getUid() {
        return uid;
    }

    public ArrayList<String> getClasses() {
        return classes;
    }

    // Chuyển đổi đối tượng Response sang JSON thủ công
    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"timestamp\":").append(timestamp).append(",");
        json.append("\"uid\":\"").append(uid).append("\",");
        json.append("\"topIndex\":[");

        for (int i = 0; i < classes.size(); i++) {
            json.append("\"").append(classes.get(i)).append("\"");
            if (i < classes.size() - 1) {
                json.append(",");
            }
        }

        json.append("]");
        json.append("}");
        return json.toString();
    }
}