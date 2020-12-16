package flinkSql.sink;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author zhengyonghong
 * @create 2020--12--16--16:12
 */
public class Flink_Sink_File {
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
        Table tableResult = table.select("id,temp");

        //SQL风格
        tableEnv.createTemporaryView("sensor",mapDS);
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor");

        //创建文件连接器
        tableEnv.connect(new FileSystem().path("tableoutput"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("tableOutTable");

        tableEnv.connect(new FileSystem().path("sqloutput"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("sqlOutTable");

        //将数据写入文件系统
        tableEnv.insertInto("tableOutTable",tableResult);
        tableEnv.insertInto("sqlOutTable",sqlResult);

        //执行
        env.execute();
    }
}
