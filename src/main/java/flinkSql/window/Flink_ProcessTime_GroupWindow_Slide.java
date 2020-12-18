package flinkSql.window;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author zhengyonghong
 * @create 2020--12--18--10:13
 */
public class Flink_ProcessTime_GroupWindow_Slide {
    public static void main(String[] args) {
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

        //基于时间的滚动窗口
        Table table = tableEnv.fromDataStream(mapDS, "id,ts,temp,pt.proctime");
        //table API
        //滚动窗口
        Table tableResult = table.window(Tumble.over("10.seconds").on("pt").as("tw"))
                .groupBy("tw,id")
                .select("id,id.count");



        //SQL风格


        //打印表信息
        table.printSchema();

    }
}
