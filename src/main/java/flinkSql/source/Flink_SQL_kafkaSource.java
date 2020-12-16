package flinkSql.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

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

        //连接kafka
        tableEnv.connect(new Kafka()
                        .version("0.11")
                        .topic("testTopic")
                        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
                        .property(ConsumerConfig.GROUP_ID_CONFIG,"group1"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id",DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("kafka");

        //SQL风格
        Table sqlResult = tableEnv.sqlQuery("select id,max(temp) from kafka group by id");
        tableEnv.toRetractStream(sqlResult,Row.class).print("SQL");

        //Table API DSL风格
        Table kafka = tableEnv.from("kafka");
        Table tableResult = kafka.groupBy("id").select("id,temp.max");
        tableEnv.toRetractStream(tableResult,Row.class).print("DSL");

        //执行
        env.execute();
    }
}
