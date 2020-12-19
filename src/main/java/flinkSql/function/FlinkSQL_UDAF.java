package flinkSql.function;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author zhengyonghong
 * @create 2020--12--18--15:53
 */
//TODO 求最高温度
public class FlinkSQL_UDAF {
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
        tableEnv.registerFunction("tempAvg",new MyUDAFAvg());

        //Table API UDAF    sensor_1 => [sensor 6, 1,1]
        Table tableResult = table.groupBy("id").select("id,temp.tempAvg");

        //SQL UDAF
        Table sqlResult = tableEnv.sqlQuery("select id,tempAvg(temp) from " + table + " group by id");

        //打印测试
        tableEnv.toRetractStream(tableResult, Row.class).print("table");
        tableEnv.toRetractStream(sqlResult, Row.class).print("sql");

        //启动
        env.execute();
    }
    public static class MyUDAFAvg extends AggregateFunction<Double,Tuple2<Double,Integer>>{

        //最终计算结果方法
        @Override
        public Double getValue(Tuple2<Double, Integer> doubleIntegerTuple2) {
            return doubleIntegerTuple2.f0/doubleIntegerTuple2.f1;
        }

        //初始化缓冲区
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        //每进来一条数据进行累加
        public void accumulate(Tuple2<Double, Integer> acc,Double value){
            acc.f0 += value;
            acc.f1 += 1;
        }
    }

}
