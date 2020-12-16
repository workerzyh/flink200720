package process;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import javax.xml.bind.PrintConversionEvent;
import java.security.PrivateKey;

/**
 * @author zhengyonghong
 * @create 2020--12--14--14:33
 */
public class Flink_Process_wordcount {
    public static void main(String[] args) throws Exception {
        //TODO 用process做wordcount
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取端口数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //压平
        SingleOutputStreamOperator<String> flatMapProcess = stream.process(new MyFlatMapProcess());


        //转换kv
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapProcess = flatMapProcess.process(new MyMapProcess());


        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = mapProcess.keyBy(0);

        //累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = tuple2TupleKeyedStream.process(new MySumProcess());

        //打印测试
        result.print();

        //启动
        env.execute();

    }
    //TODO 压平操作
    public static class MyFlatMapProcess extends ProcessFunction<String,String>{

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        }
    }
    //TODO 转换kv
    public static class MyMapProcess extends ProcessFunction<String,Tuple2<String,Integer>>{

        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value,1));
        }
    }

    //TODO 累加count
    public static class MySumProcess extends KeyedProcessFunction<Tuple,Tuple2<String, Integer>,Tuple2<String, Integer>>{
        //状态声明
        private ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //状态初始化
            countState=getRuntimeContext().getState(new ValueStateDescriptor<Integer>("word_count",Integer.class,0));
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            //获取状态
            Integer lastCount = countState.value();
            //更新状态
            countState.update(lastCount+value.f1);

            //累加count并写出
            out.collect(new Tuple2<>(value.f0,countState.value()));

        }
    }


}
