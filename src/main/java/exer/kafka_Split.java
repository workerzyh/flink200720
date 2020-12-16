package exer;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collections;
import java.util.Properties;

/**
 * @author zhengyonghong
 * @create 2020--12--12--8:56
 */
public class kafka_Split {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put("auto.offset.reset","latest");

        //读取kafka数据
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>("testTopic", new SimpleStringSchema(), props));

        //转换成传感器
        SingleOutputStreamOperator<SensorReading> mapDS = kafkaDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //分流
        SplitStream<SensorReading> splitDS = mapDS.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemp() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        //选择流
        DataStream<SensorReading> high = splitDS.select("high");
        DataStream<SensorReading> low = splitDS.select("low");

        //打印测试
        high.print("high");
        low.print("low");

        //启动
        env.execute();


    }
}
