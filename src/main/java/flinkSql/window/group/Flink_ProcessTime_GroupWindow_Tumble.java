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
//TODO 处理时间滚动窗口
public class Flink_ProcessTime_GroupWindow_Tumble {
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

        //转换表，添加时间戳
        Table table = tableEnv.fromDataStream(SensorDS, "id,ts,temp,pt.proctime");

        //开窗 Table API风格
        Table tableResult = table.window(Tumble.over("10.seconds").on("pt").as("tw"))
                .groupBy("id,tw")
                .select("id,id.count");

        //SQL风格
        tableEnv.createTemporaryView("sensortable",table);
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) from sensortable group by id,tumble(pt,interval '10' second)");

        //打印结果
        tableEnv.toAppendStream(tableResult, Row.class).print("table");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sql");

        //启动
        env.execute();

    }
}
