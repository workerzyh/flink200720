package exer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author zhengyonghong
 * @create 2020--12--14--8:40
 */
public class wordToMySQL {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"groupid");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put("auto.offset.reset","latest");

        //读取kafka数据
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>("testTopic", new SimpleStringSchema(), props));

        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByDS = wordToOne.keyBy(0);

        //累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyByDS.sum(1);

        //写入mysql
        DataStreamSink<Tuple2<String, Integer>> MySQLDS = result.addSink(new MySQLFunc());

        //启动
        env.execute();
    }
    public static class MySQLFunc extends RichSinkFunction<Tuple2<String,Integer>>{
        //SQL连接
        private Connection connection;
        //预编译体
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","123456");

            preparedStatement = connection.prepareStatement("insert into wordcount values(?,?) on duplicate key update count = ?");
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            //设置参数
            preparedStatement.setString(1,value.f0);
            preparedStatement.setInt(2,value.f1);
            preparedStatement.setInt(3,value.f1);

            //执行SQL
            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
