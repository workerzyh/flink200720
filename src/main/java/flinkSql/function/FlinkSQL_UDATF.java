package flinkSql.function;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;


/**
 * @author zhengyonghong
 * @create 2020--12--18--15:53
 */
//TODO 求最高温度
public class FlinkSQL_UDATF {
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
        tableEnv.registerFunction("top2",new MyUDATFTop2());


        //Table API UDATF    sensor_1 => [sensor 6, 1,1]
        Table tableResult = table.groupBy("id")
                .flatAggregate("top2(temp) as (temp2,rank2)")
                .select("id,temp2,rank2");

        //打印测试
        tableEnv.toRetractStream(tableResult, Row.class).print("table");

        //启动
        env.execute();
    }
    public static class MyUDATFTop2 extends TableAggregateFunction<Tuple2<Double,Integer>,Tuple2<Double,Double>>{

        //初始化缓冲
        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return new Tuple2<>(Double.MIN_VALUE,Double.MIN_VALUE);
        }

        //每进一条数据进行比较
        public void accumulate(Tuple2<Double,Double> acc,Double v){
            if(v > acc.f0){
                acc.f1 = acc.f0;
                acc.f0 = v;
            }else if(v > acc.f1){
                acc.f1 = v;
            }
        }

        //发送数据方法
        public void emitValue(Tuple2<Double,Double> acc, Collector<Tuple2<Double,Integer>> out){
            out.collect(new Tuple2<>(acc.f0,1));
            if (acc.f1 != Double.MIN_VALUE){
                out.collect(new Tuple2<>(acc.f1,2));
            }
        }

    }

}
