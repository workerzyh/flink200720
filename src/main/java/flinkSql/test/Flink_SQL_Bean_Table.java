package flinkSql.test;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zhengyonghong
 * @create 2020--12--16--11:45
 */
public class Flink_SQL_Bean_Table {
    public static void main(String[] args) throws Exception {
        //获取执行环境
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

        //创建表
        tableEnv.createTemporaryView("sensor",mapDS);

        //1.SQL方式实现查询
        Table sqlTable = tableEnv.sqlQuery("select id,temp from sensor");

        //1.表转换为流进行输出
        //DataStream<Row> rowDataStream = tableEnv.toAppendStream(sqlTable, Row.class);
        tableEnv.toRetractStream(sqlTable,Row.class).print("retract");
       // rowDataStream.print();

        //1.执行
        env.execute();


    }
}
