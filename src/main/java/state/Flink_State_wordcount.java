package state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import sun.java2d.pipe.ValidatePipe;

/**
 * @author zhengyonghong
 * @create 2020--12--14--14:33
 */
public class Flink_State_wordcount {
    public static void main(String[] args) throws Exception {
        //TODO 使用状态编程实现wordcount
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 每10s保存一次状态
        env.enableCheckpointing(10000L);
        //读取端口数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

        //利用状态编程实现wordcount
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.map(new MyStateFunc());

        //打印测试
        result.print();

        //启动
        env.execute();
    }
    public static class MyStateFunc extends RichMapFunction<Tuple2<String, Integer>,Tuple2<String, Integer>>{
        //声明状态
        private ValueState<Integer> count;

        @Override
        public void open(Configuration parameters) throws Exception {
            //状态初始化
            count = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count",Integer.class,0));
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
            //获取状态
            Integer lastCount = count.value();
            //更新状态
            count.update(lastCount+value.f1);

            return new Tuple2<>(value.f0,count.value());
        }
    }

}

