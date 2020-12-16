package exer;

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
 * @create 2020--12--15--18:18
 */
//TODO 需求：温度超过10s未下降，预警
public class Temp_Incr_10_Timer {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从端口获取数据并创建流
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);

        //转换成javabean
        SingleOutputStreamOperator<SensorReading> sensorDS = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //分组
        KeyedStream<SensorReading, Tuple> keyByDS = sensorDS.keyBy("id");

        //温度监控
        SingleOutputStreamOperator<String> result = keyByDS.process(new TempIncrTimerProcess(10));

        //打印测试
        result.print();

        //启动
        env.execute();
    }
    //TODO 方案：存储温度和时间状态(存储定时器的时间)，每次数据进来判断
    // 当温度比之前高，且时间状态为空，设置定时器
    // 当温度比之前低，且时间状态不空，删除定时器
    // 定时器触发后也必须清空时间状态，否则温度上升不会触发定时器
    public static class TempIncrTimerProcess extends KeyedProcessFunction<Tuple,SensorReading,String>{
        //传入的定时器触发间隔
        private Integer maxRoll;

        public TempIncrTimerProcess(Integer maxRoll) {
            this.maxRoll = maxRoll;
        }

        //声明温度状态
        private ValueState<Double> tempState;

        //声明时间状态
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //温度和时间状态初始化
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp_state",Double.class,Double.MIN_VALUE));

            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts_state",Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //获取状态温度
            Double stateTempHis = tempState.value();

            //获取当前传入温度
            Double curTemp = value.getTemp();

            //获取历史定时器事件
            Long tsHis = tsState.value();

            //当当前温度比历史温度高，且时间状态为空，设置定时器
            if(curTemp>stateTempHis && tsHis==null){
                //获取当前时间
                Long curTs = ctx.timerService().currentProcessingTime() + maxRoll*1000L;
                //设定定时器
                ctx.timerService().registerProcessingTimeTimer(curTs);

                //更新温度和定时器时间状态
                tempState.update(curTemp);
                tsState.update(curTs);

            }else if(curTemp < stateTempHis && tsHis !=null){
                // 当温度比之前低，且时间状态不空，删除定时器
                ctx.timerService().deleteProcessingTimeTimer(tsHis);

                //清空定时时间状态
                tsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //输出数据
            out.collect(ctx.getCurrentKey() + "号传感器超过10s未降温");

            //清空定时器时间戳
            tsState.clear();
        }
    }
}
