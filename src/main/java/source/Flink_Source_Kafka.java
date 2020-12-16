package source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author zhengyonghong
 * @create 2020--12--11--11:06
 */
public class Flink_Source_Kafka {
    public static void main(String[] args) throws Exception {
    //TODO 从kafka读取数据流
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //kafka配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put("auto.offset.reset","latest");

        //从kafka读取数据流
        DataStreamSource<String> testTopic = env.addSource(new FlinkKafkaConsumer011<String>("testTopic", new SimpleStringSchema(), props));

        //打印测试
        testTopic.print();

        //启动
        env.execute();

    }
}
