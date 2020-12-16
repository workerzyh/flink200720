package process;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.security.Key;

/**
 * @author zhengyonghong
 * @create 2020--12--14--14:33
 */
public class Flink_Process_OnTimer {
    public static void main(String[] args) throws Exception {
        //用Process实现定时器，每2s触发一次

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取端口数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //分组
        KeyedStream<String, String> KeyedStream = stream.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });

        //调用定时器
        SingleOutputStreamOperator<String> result = KeyedStream.process(new MyKeyedProcess());

        //打印测试
        result.print();

        //启动
        env.execute();

    }

    public static class MyKeyedProcess extends KeyedProcessFunction<String,String,String>{

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            //输出数据
            out.collect(value);

            //注册定时器
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 2000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("2s后触发定时器");
        }
    }

}
