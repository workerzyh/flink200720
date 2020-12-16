package sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


/**
 * @author zhengyonghong
 * @create 2020--12--11--16:42
 */
public class Flink_sink_Redis {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //创建redis信息
        FlinkJedisPoolConfig jedis = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        //写入redis
        stream.addSink(new RedisSink<>(jedis,new MyRedisMapper()));

        //启动任务
        env.execute();

    }
    public static class MyRedisMapper implements RedisMapper<String>{
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(String data) {
            return data;
        }

        @Override
        public String getValueFromData(String data) {
            return data;
        }
    }

}
