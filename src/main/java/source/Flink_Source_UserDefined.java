package source;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.*;

/**
 * @author zhengyonghong
 * @create 2020--12--11--11:20
 */
public class Flink_Source_UserDefined {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取数据
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new MySource());

        //打印测试
        sensorReadingDataStreamSource.print();

        //启动任务
        env.execute();

    }
    //自定义source
    public static class MySource implements SourceFunction<SensorReading>{
        //定义标志，标识是否取消source
        Boolean running = true;

        //随机准备
        Random random = new Random();

        //存放传感数据的数据得集合
        private Map<String, SensorReading> map = new HashMap<>();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            //随机生成传感数据
            for (int i = 0; i < 10; i++) {
                String id = "sensor" + (i+1);
                map.put(id,new SensorReading(id,System.currentTimeMillis(),60D+random.nextGaussian()*20));
            }
            //造数据
            while (running){
                for (String id : map.keySet()) {
                    //拿到上次数据
                    SensorReading sensorReading = map.get(id);
                    //获取上次温度
                    Double lastTemp = sensorReading.getTemp();
                    //生成新数据
                    SensorReading sensorReading1 = new SensorReading(id, System.currentTimeMillis(), lastTemp + random.nextGaussian());
                    //写出数据
                    ctx.collect(sensorReading1);
                    //放回map中（改变基准）
                    map.put(id,sensorReading1);
                }
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            running=false;
        }
    }

}
