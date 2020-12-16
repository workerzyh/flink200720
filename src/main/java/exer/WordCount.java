package exer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

/**
 * @author zhengyonghong
 * @create 2020--12--12--8:43
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据流
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);

        //压平数据
        SingleOutputStreamOperator<String> word = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = word.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return sumRedis(value);
            }
        });

        //打印测试
        result.print();

        //启动
       env.execute();
    }
    public static Tuple2<String,Integer> sumRedis(String word){
        //获取redis连接
        Jedis jedisClient = RedisUtil.getJedisClient();

        //检查redis中是否存在
        if (jedisClient.exists(word)){
            jedisClient.incr(word);
           // jedisClient.set(word,String.valueOf(Integer.parseInt(jedisClient.get(word)) + 1));
         }else{
            addRedis(word);
        }
        //关闭
        jedisClient.close();

        //返回数据
        return new Tuple2<>(word,Integer.parseInt(jedisClient.get(word)));
    }

    public static void addRedis(String word){
        //获取redis连接
        Jedis jedisClient = RedisUtil.getJedisClient();

        //检查redis中是否存在
        jedisClient.set(word,"1");

        //关闭
        jedisClient.close();

    }
}
