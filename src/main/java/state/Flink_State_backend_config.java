package state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhengyonghong
 * @create 2020--12--15--16:24
 */
public class Flink_State_backend_config {
    public static void main(String[] args) {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 设置状态后端
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102/flink/flinkck"));

        //TODO CK配置
        //1.开启
        env.enableCheckpointing(10000L);
        //2.设置两次CK开启间隔时间
        env.getCheckpointConfig().setCheckpointInterval(500L);
        //3.设置CK模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //4.设置最多同时开启的ck任务
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);
        //5.设置CK超时时间
        env.getCheckpointConfig().setCheckpointTimeout(1000L);
        //6.设置CK重试次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        //7.设置两个CK最小间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        //8.如果存在更近的savapoint，则设置是否从savapoint恢复
        env.getCheckpointConfig().setPreferCheckpointForRecovery(false);

        //TODO CK重启策略
        //1.固定延迟重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        //2.失败率重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(50),Time.seconds(5)));



    }
}
