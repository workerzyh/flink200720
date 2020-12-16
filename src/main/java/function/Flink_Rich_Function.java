package function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhengyonghong
 * @create 2020--12--11--16:14
 */
public class Flink_Rich_Function {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = stream.flatMap(new MyRichMapFunc());
        wordToOne.keyBy(0).sum(1).print();

        env.execute();


    }


    public static class MyRichMapFunc extends RichFlatMapFunction<String, Tuple2<String,Integer>>{
       //生命周期方法
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            //获取运行时上下文
            RuntimeContext runtimeContext = getRuntimeContext();

            //状态编程
            ValueState<Object> valuestate = runtimeContext.getState(new ValueStateDescriptor<Object>("", Object.class));
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] fields = value.split(" ");
            for (String field : fields) {
                out.collect(new Tuple2<>(field,1));
            }
        }

        //生命周期方法
        @Override
        public void close() throws Exception {
            super.close();
        }
    }

}
