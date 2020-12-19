package flinkSql.window.group;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zhengyonghong
 * @create 2020--12--18--10:13
 */
//TODO 处理时间会话窗口
public class Flink_ProcessTime_GroupWindow_Session {
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


        //转table添加处理时间字段
        Table table = tableEnv.fromDataStream(mapDS, "id,ts,temp,pt.proctime");

        //table API风格
        Table tableResult = table.window(Session.withGap("10.seconds").on("pt").as("sw"))
                .groupBy("sw,id")
                .select("id,id.count");

        //SQL风格
        tableEnv.createTemporaryView("sqlTable",table);
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) from sqlTable group by id,session(pt,interval '10' second)");

        //表转流打印结果
        tableEnv.toAppendStream(tableResult, Row.class).print("table");
        tableEnv.toAppendStream(sqlResult,Row.class).print("sql");

        //执行
        env.execute();

    }
}
