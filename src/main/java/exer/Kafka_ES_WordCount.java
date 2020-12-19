package exer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author zhengyonghong
 * @create 2020--12--18--8:31
 */
public class Kafka_ES_WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");
        props.put("auto.offset.reset","latest");

        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>("testTopic", new SimpleStringSchema(), props));

        SingleOutputStreamOperator<WordCountBean> wordToOne = kafkaDS.flatMap(new FlatMapFunction<String, WordCountBean>() {
            @Override
            public void flatMap(String value, Collector<WordCountBean> out) throws Exception {
                String[] split = value.split(",");
                for (String word : split) {
                    out.collect(new WordCountBean(word, 1));
                }
            }
        });

        tableEnv.createTemporaryView("wordcount",wordToOne);

        Table sqlResult = tableEnv.sqlQuery("select word,sum(count1) from wordcount group by word");

        tableEnv.connect(new Elasticsearch()
                        .version("6")
                        .host("hadoop102",9200,"http")
                        .index("kafka_es_wordcount")
                        .documentType("_doc")
                        .bulkFlushMaxActions(1))
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("word", DataTypes.STRING())
                        .field("count",DataTypes.INT()))
                .inUpsertMode()
                .createTemporaryTable("esTable");

        tableEnv.insertInto("esTable",sqlResult);

        //启动
        env.execute();




    }
}
