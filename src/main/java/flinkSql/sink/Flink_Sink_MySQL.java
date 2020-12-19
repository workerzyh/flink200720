package flinkSql.sink;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author zhengyonghong
 * @create 2020--12--18--9:16
 */
public class Flink_Sink_MySQL {
    public static void main(String[] args) throws Exception {
        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //获取数据并转换bean
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<SensorReading> mapDS = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //对流注册一个表
        Table table = tableEnv.fromDataStream(mapDS);

        //table API DSL风格
        Table tableResult = table.groupBy("id").select("id,temp.max");

        //SQL风格
        tableEnv.createTemporaryView("sensor",mapDS);
        Table sqlResult = tableEnv.sqlQuery("select id,min(temp) from sensor group by id");

        //创建MySQL
        String sinkDDL = "create table jdbcOutputTable (" +
                " id varchar(20) not null, " +
                " temp double not null " +
                ") with (" +
                " 'connector.type' = 'jdbc', " +
                " 'connector.url' = 'jdbc:mysql://hadoop102:3306/test', " +
                " 'connector.table' = 'sensor_id', " +
                " 'connector.driver' = 'com.mysql.jdbc.Driver', " +
                " 'connector.username' = 'root', " +
                "  'connector.write.flush.max-rows' = '1', " +
                " 'connector.password' = '123456' )";
        //变成连接器
        tableEnv.sqlUpdate(sinkDDL);

        //将数据写入文件系统
        tableEnv.insertInto("jdbcOutputTable",tableResult);
        //tableEnv.insertInto("ESTable2",sqlResult);
        //sqlResult.insertInto("jdbcOutputTable");

        //执行
        env.execute();
    }
}
