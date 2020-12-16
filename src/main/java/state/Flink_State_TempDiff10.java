package state;

import bean.SensorReading;
import exer.Distinct_Word;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
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

/**
 * @author zhengyonghong
 * @create 2020--12--14--14:33
 */
public class Flink_State_TempDiff10 {
    public static void main(String[] args) throws Exception {
        //TODO 利用状态编程实现当两次温差大于10时
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //读取端口数据创建流
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //转换bean，同时分组
        KeyedStream<SensorReading, Tuple> keyByDS = stream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        }).keyBy("id");

        //适用richfunction实现状态编程，连续两次温差大于10，报警
        SingleOutputStreamOperator<String> result = keyByDS.flatMap(new MyFlatMapRichFunc(10.0));

        //打印测试
        result.print();

        //启动
        env.execute();

    }

    public static class MyFlatMapRichFunc extends RichFlatMapFunction<SensorReading,String>{

        private Double maxDiff;

        public MyFlatMapRichFunc(Double maxDiff) {
            this.maxDiff = maxDiff;
        }
        //状态声明
        private ValueState<Double> tempState;
        @Override
        public void open(Configuration parameters) throws Exception {
            //状态初始化
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("sensor-temp", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<String> out) throws Exception {
            //获取状态值
            Double temp = tempState.value();
            //逻辑处理
            if(temp!=null&&(Math.abs(temp-value.getTemp())>10.0)){
                out.collect("近两次温差大于"+maxDiff+"度");

            }
            //状态更新
            tempState.update(value.getTemp());
        }
    }

}
