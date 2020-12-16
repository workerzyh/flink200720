package flinkSql.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import javax.xml.crypto.Data;

/**
 * @author zhengyonghong
 * @create 2020--12--16--14:48
 */
//  TODO 从kafka获取
public class Flink_SQL_kafkaSource {
    public static void main(String[] args) throws Exception {
        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //kafka连接器
        tableEnv.connect(new Kafka()
                            .version("0.11")
                            .topic("testTopic")
                            .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
                            .property(ConsumerConfig.GROUP_ID_CONFIG,"group2"))
                .withFormat(new Json())
                .withSchema(new Schema()
                            .field("id",DataTypes.STRING())
                            .field("ts",DataTypes.BIGINT())
                            .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("kafka");

        //创建表
        Table kafka = tableEnv.from("kafka");

        //DSL查询
        Table tableDSL = kafka.groupBy("id").select("id,temp.max");

        //SQL查询
        Table tableSQl = tableEnv.sqlQuery("select id,min(temp) from kafka group by id");

        //转换成流输出
        tableEnv.toRetractStream(tableDSL,Row.class).print("DSL");
        tableEnv.toRetractStream(tableSQl,Row.class).print("SQL");

        //执行
        env.execute();
    }
}
