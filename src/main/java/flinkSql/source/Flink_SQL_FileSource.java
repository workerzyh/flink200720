package flinkSql.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author zhengyonghong
 * @create 2020--12--16--14:18
 */
public class Flink_SQL_FileSource {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //构建文件连接器
        tableEnv.connect(new FileSystem().path("input/sensorreading.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                            .field("id",DataTypes.STRING())
                            .field("ts",DataTypes.BIGINT())
                            .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("sensor");

        //获取表
        Table sensor = tableEnv.from("sensor");

        //table API查询 DSL风格
        Table tableResult = sensor.groupBy("id").select("id,temp.max");

        //SQL风格
        Table sqlResult = tableEnv.sqlQuery("select id,min(temp) from sensor group by id");

        //查询结果转换成流输出
        tableEnv.toRetractStream(tableResult, Row.class).print("DSL");

        tableEnv.toRetractStream(sqlResult,Row.class).print("SQL");

        //执行
        env.execute();

    }
}
