package exer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

/**
 * @author zhengyonghong
 * @create 2020--12--12--9:07
 */
public class Distinct_Word {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文本数据
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);

        //拆分
        SingleOutputStreamOperator<String> word = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                out.collect(value);
            }
        });

        //过滤
        SingleOutputStreamOperator<String> result = word.filter(new MyFilterFunc());

        //打印
        result.print();

        //启动
        env.execute();

    }
    public static class MyFilterFunc extends RichFilterFunction<String> {
        private Jedis jedis;
        @Override
        public void open(Configuration parameters) throws Exception {
            jedis=new Jedis("hadoop102",6379);
        }

        @Override
        public boolean filter(String value) throws Exception {
            if(jedis.exists(value)){
                return false;
            }
            jedis.set(value,value);
            return true;
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }
}



