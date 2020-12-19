package flinkSql.function;

import bean.SensorReading;
import javafx.scene.control.TableFocusModel;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author zhengyonghong
 * @create 2020--12--18--15:53
 */
public class FlinkSQL_UDTF {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //从端口获取数据流并转换
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<SensorReading> SensorDS = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //转换表
        Table table = tableEnv.fromDataStream(SensorDS);

        //注册函数
        tableEnv.registerFunction("udtfSplit",new MyUDTFSplit());

        //Table API UDTF    sensor_1 => [sensor 6, 1,1]
        Table tableResult = table.joinLateral("udtfSplit(id) as (word,length)")
                .select("id,word,length");

        //SQL UDTF
        Table sqlResult = tableEnv.sqlQuery("select id,word,length from " + table + ", lateral table(udtfSplit(id)) as splitTable(word,length)");


        //打印测试
        tableEnv.toAppendStream(tableResult,Row.class).print("table");
        tableEnv.toAppendStream(sqlResult,Row.class).print("sql");

        //启动
        env.execute();
    }
    public static class MyUDTFSplit extends TableFunction<Tuple2<String,Integer>>{
        public void eval(String value){
            String[] s = value.split("_");
            for (String s1 : s) {
                collect(new Tuple2<>(s1,s1.length()));
            }
        }

    }}
