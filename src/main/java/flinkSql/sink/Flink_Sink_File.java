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

        //从端口获取数据转换bean
        SingleOutputStreamOperator<SensorReading> inputDS = env.socketTextStream("hadoop102", 9999).map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        /*//流注册
        tableEnv.createTemporaryView("fileSensor",inputDS);
        //SQL风格
        Table sqlResult = tableEnv.sqlQuery("select id,temp from fileSensor");

        //构建文件连接器
        tableEnv.connect(new FileSystem().path("sqloutput1"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                     .field("id",DataTypes.STRING())
                    .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("sqltable1");

        tableEnv.insertInto("sqltable1",sqlResult);*/

        //DSL风格
        Table table = tableEnv.fromDataStream(inputDS);
        Table tableResult = table.select("id,temp");
        tableEnv.connect(new FileSystem().path("tableoutput1"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id",DataTypes.STRING())
                        .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("table2");
        tableEnv.insertInto("table2",tableResult);


        //执行
        env.execute();
    }
}
