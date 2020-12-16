package sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author zhengyonghong
 * @create 2020--12--11--16:27
 */
public class Flink_sink_Kafka {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //kafka配置信息
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        props.put(ProducerConfig.ACKS_CONFIG,"-1");

        //写入kafka
        stream.addSink(new FlinkKafkaProducer011<String>("testTopic",new SimpleStringSchema(),props));

        //执行
        env.execute();
    }
}
