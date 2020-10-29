package io.eventador;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;


public class FlinkReadKafka {

    private static final int CHECKPOINT_INTERVAL = 300000;

    private static final int RESTART_DELAY = 10000;
    private static final int RESTART_ATTEMPTS = 4;
    private static final String READ_TOPIC_PARAM = "read-topic";
    private static final String WRITE_TOPIC_PARAM = "write-topic";

    public static void main(String[] args) throws Exception {
        // Read parameters from command line
        final ParameterTool params = ParameterTool.fromArgs(args);

        if(params.getNumberOfParameters() < 3) {
            System.out.println("\nUsage: FlinkReadKafka --read-topic <topic> --write-topic <topic> --bootstrap.servers <kafka brokers> --group.id <groupid>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        /**
            Configure stream execution environment
         */

        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(RESTART_ATTEMPTS, RESTART_DELAY));
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(CHECKPOINT_INTERVAL); // 300 seconds
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        String sourceTopic = params.getRequired(READ_TOPIC_PARAM);
        String writeTopic = params.getRequired(WRITE_TOPIC_PARAM);
        SimpleStringSchema schema = new SimpleStringSchema();


        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(sourceTopic, schema, params.getProperties());
        consumer.assignTimestampsAndWatermarks(new CucumberWatermarkStrategy());
        DataStream<String> messageStream = env.addSource(consumer);


        SingleOutputStreamOperator<Object> processedWindow = messageStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
                .process(new ProcessAllWindowFunction<String, Object, TimeWindow>() {
                    ObjectMapper MAPPER = new ObjectMapper();
                    String WINDOW_ID = "window_id";

                    @Override
                    public void process(Context context, Iterable<String> iterable, Collector<Object> collector) throws Exception {
                        String window_id = UUID.randomUUID().toString();
                        iterable.forEach(jsonString -> {
                            try {
                                Map<String, String> map = MAPPER.readValue(jsonString, Map.class);
                                map.put(WINDOW_ID, window_id);
                                String output_string = MAPPER.writeValueAsString(map);
                                collector.collect(output_string);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                    }
                }).name("WindowProcessFunction");


         // Print Kafka messages to stdout - will be visible in logs
        processedWindow.print();

        processedWindow.addSink(new FlinkKafkaProducer010(
                writeTopic,
                new SimpleStringSchema(),
                params.getProperties()
        )).name("Write To Kafka");

        env.execute("FlinkReadWriteWindowKafka");
    }




    public static class CucumberWatermarkStrategy implements WatermarkStrategy{

        @Override
        public WatermarkGenerator createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new FlinkReadKafka.BoundedOutOfOrdernessGenerator<>();
        }

        @Override
        public TimestampAssigner createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new FlinkReadKafka.KafkaTimestampAssigner();
        }
    }


    public static class KafkaTimestampAssigner implements TimestampAssigner<String> {
        @Override
        public long extractTimestamp(String event, long previousElementTimestamp) {
            return previousElementTimestamp;
        }
    }

    public static class BoundedOutOfOrdernessGenerator<Event> implements WatermarkGenerator<Event> {

        private final long maxOutOfOrderness = 3500; // 3.5 seconds
        private long currentMaxTimestamp;


        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput watermarkOutput) {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
        }

    }
}



