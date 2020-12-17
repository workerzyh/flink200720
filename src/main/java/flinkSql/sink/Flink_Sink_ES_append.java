package flinkSql.sink;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author zhengyonghong
 * @create 2020--12--16--16:12
 */
public class Flink_Sink_ES_append {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //从端口获取数据流并转换
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> mapDS = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //转换table API进行查询
        Table table = tableEnv.fromDataStream(mapDS);
        //查询数据
        Table tableResult = table.select("id,temp");

        tableEnv.connect(new Elasticsearch()
                        .version("6")
                        .host("hadoop102",9200,"http")
                        .index("flink_sink_es_append1")
                        .documentType("_doc")
                        .bulkFlushMaxActions(1))
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id",DataTypes.STRING())
                        .field("temp",DataTypes.DOUBLE()))
                .inAppendMode()
                .createTemporaryTable("esTable1");

        tableEnv.insertInto("esTable1",tableResult);

        //执行
        env.execute();
    }
}
