package process;

import akka.io.Udp;
import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @author zhengyonghong
 * @create 2020--12--15--10:20
 */
//TODO 实现10s温度没有下降则报警
public class Flink_Process_State_Ontimer {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //读取端口数据
        DataStreamSource<String> inputDS = env.socketTextStream("hadoop102", 9999);

        //转换bean
        SingleOutputStreamOperator<SensorReading> map = inputDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //分组
        KeyedStream<SensorReading, Tuple> id = map.keyBy("id");

        //使用process实现10s温度没有下降则报警
        SingleOutputStreamOperator<String> process = id.process(new MyKeyedProcessFunc(10));

        //打印测试
        process.print();

        //启动
        env.execute();

    }
    //TODO 使用process实现10s温度没有下降则报警
    public static class MyKeyedProcessFunc extends KeyedProcessFunction<Tuple,SensorReading,String>{
        //定义时间间隔
        private Integer interval;

        public MyKeyedProcessFunc(Integer interval) {
            this.interval = interval;
        }

        //TODO 声明温度状态用于存储每次的温度值
        private ValueState<Double> temp;

        //TODO 声明时间状态用于存放定时器时间
        private ValueState<Long> tsState;

        //状态初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            temp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp_state",Double.class,Double.MIN_VALUE));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts_state",Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //获取上次温度值
            Double lastTemp = temp.value();

            //获取定时器时间状态
            Long lastTs = tsState.value();

            //获取当前传入温度
            Double currentTemp = value.getTemp();

            //定时时间,当前时间+interval
            long timerTs = ctx.timerService().currentProcessingTime() + interval * 1000L;


            //温度上升并且时间状态为Null，则注册定时器
            if (currentTemp >lastTemp && lastTs ==null){
                //注册定时器
                ctx.timerService().registerProcessingTimeTimer(timerTs);

                //更新温度和时间状态
                temp.update(currentTemp);
                tsState.update(ctx.timerService().currentProcessingTime());

            }
            //当温度下降时，删除定时器
            if (currentTemp <lastTemp && lastTs !=null){
                //删除定时器
                ctx.timerService().deleteProcessingTimeTimer(lastTs);

                //清空时间状态
                tsState.clear();

            }
        }

        //TODO 定时器触发
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //输出数据
            out.collect(ctx.getCurrentKey()+"连续10s温度没有下降");

            //清空时间状态
            tsState.clear();
        }
    }
}
