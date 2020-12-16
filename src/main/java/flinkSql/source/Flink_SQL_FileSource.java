package flinkSql.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
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
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                    .field("id",DataTypes.STRING())
                    .field("ts",DataTypes.BIGINT())
                    .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("fileSensor");

        /*//SQL风格
        // 转换table
        Table table = tableEnv.sqlQuery("select id,min(temp) from fileSensor group by id");
        //table结果转换成流输出
        tableEnv.toRetractStream(table,Row.class).print("Sql");*/

        //table API风格
        //创建表
        Table fileSensor = tableEnv.from("fileSensor");
        Table tableResult = fileSensor.groupBy("id").select("id,temp.min");
        tableEnv.toRetractStream(tableResult,Row.class).print("DSL");

        //任务启动
        env.execute();

    }
}
