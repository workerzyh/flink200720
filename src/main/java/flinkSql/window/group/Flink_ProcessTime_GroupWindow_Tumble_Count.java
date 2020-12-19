package flinkSql.window.group;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zhengyonghong
 * @create 2020--12--18--10:13
 */
//TODO 处理时间计数窗口
public class Flink_ProcessTime_GroupWindow_Tumble_Count {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //从端口获取数据流并转换
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> sensorDS = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

       //转换表添加处理时间
        Table table = tableEnv.fromDataStream(sensorDS, "id,ts,temp,pt.proctime");

        //table API
        Table tableResult = table.window(Tumble.over("10.rows").on("pt").as("tw"))
                .groupBy("tw,id")
                .select("id,id.count");


        //表转换流输出结果
        tableEnv.toAppendStream(tableResult, Row.class).print("table");

        //执行
        env.execute();
    }
}
