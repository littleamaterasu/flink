import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFlink {
    private static final Logger LOG = LoggerFactory.getLogger(TestFlink.class);

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define two input streams
        DataStream<String> input1 = env.socketTextStream("host.docker.internal", 9999);
        DataStream<String> input2 = env.socketTextStream("host.docker.internal", 9998);

        // Map input strings to integers
        DataStream<Integer> intStream1 = input1.map(new ParseIntMapFunction());
        DataStream<Integer> intStream2 = input2.map(new ParseIntMapFunction());

        // Connect the two streams
        ConnectedStreams<Integer, Integer> connectedStreams = intStream1.connect(intStream2);

        // Process the connected streams
        DataStream<Integer> summedStream = connectedStreams.process(new SumAndLogCoProcessFunction());

        // Execute the Flink job
        env.execute("Sum and Log Flink Job");
    }

    /**
     * A MapFunction to parse integers from strings.
     */
    private static class ParseIntMapFunction implements MapFunction<String, Integer> {
        @Override
        public Integer map(String value) throws Exception {
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid input, not an integer: {}", value);
                return 0; // Default to 0 for invalid inputs
            }
        }
    }

    /**
     * A CoProcessFunction to sum integers from two streams and log the result.
     */
    private static class SumAndLogCoProcessFunction extends CoProcessFunction<Integer, Integer, Integer> {
        private int sum = 0;

        @Override
        public void processElement1(Integer value, Context ctx, Collector<Integer> out) throws Exception {
            sum += value;
            LOG.info("Current sum after input1: {}", sum);
            out.collect(sum);
        }

        @Override
        public void processElement2(Integer value, Context ctx, Collector<Integer> out) throws Exception {
            sum += value;
            LOG.info("Current sum after input2: {}", sum);
            out.collect(sum);
        }
    }
}
