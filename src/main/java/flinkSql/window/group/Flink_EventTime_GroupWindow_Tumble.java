package flinkSql.window.group;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zhengyonghong
 * @create 2020--12--18--10:13
 */
//TODO 事件时间滚动窗口
public class Flink_EventTime_GroupWindow_Tumble {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        //设置水位线
        SingleOutputStreamOperator<SensorReading> waterMarkDS = SensorDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTs() * 1000L;
            }
        });

        //转换表，添加时间戳
        Table table = tableEnv.fromDataStream(waterMarkDS, "id,ts,temp,rt.rowtime");

        //开窗 Table API风格
        Table tableResult = table.window(Tumble.over("10.seconds").on("rt").as("rw"))
                .groupBy("id,rw")
                .select("id,id.count");

        //SQL风格
        tableEnv.createTemporaryView("sensortable",table);
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) from sensortable group by id,tumble(rt,interval '10' second)");

        //打印结果
        tableEnv.toAppendStream(tableResult, Row.class).print("table");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sql");

        //启动
        env.execute();

    }
}
