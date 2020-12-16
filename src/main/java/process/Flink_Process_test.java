package process;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zhengyonghong
 * @create 2020--12--14--14:33
 */
public class Flink_Process_test {
    public static void main(String[] args) {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取端口数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //写入ES
    }
    public static class MyProcess extends ProcessFunction<String,String>{
        @Override
        public void open(Configuration parameters) throws Exception {
            //获取运行时上下文,用作状态编程
            RuntimeContext runtimeContext = getRuntimeContext();

            //
        }

        //处理进入系统的每一条数据
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            //输出数据
            out.collect("");

            //获取处理时间相关数据并注册和删除定时器
            ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(1L);
            ctx.timerService().deleteProcessingTimeTimer(1L);

            //获取事件时间相关数据并注册和删除定时器
            ctx.timerService().currentWatermark();
            ctx.timerService().registerEventTimeTimer(1L);
            ctx.timerService().deleteEventTimeTimer(1l);

            //侧输出流
           // ctx.output(new OutputTag<String>("outPutTag"){""});
        }

        //定时器触发任务进行
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }}
