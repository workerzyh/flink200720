package process;

import bean.SensorReading;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.print.SimpleDoc;

/**
 * @author zhengyonghong
 * @create 2020--12--14--14:33
 */
public class Flink_Process_slideOutput {
    public static void main(String[] args) throws Exception {
        //TODO 用Process做侧输出流，高于30度直接输出，低于30度写到侧输出流

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取端口数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //调用process
        SingleOutputStreamOperator<String> resultProcess = stream.process(new MySlideOutputProcess());

        //打印主流
        resultProcess.print("high");

        //打印侧输出流
        DataStream<String> slideDS = resultProcess.getSideOutput(new OutputTag<String>("lower_30"){});
        slideDS.print("low");
        //启动
        env.execute();

    }
    public static class MySlideOutputProcess extends ProcessFunction<String,String>{

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            String[] split = value.split(",");
            SensorReading sensorReading = new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            double temp = Double.parseDouble(split[2]);
            if (temp >= 30.0){
                out.collect(value);
            }else{
                ctx.output(new OutputTag<String>("lower_30"){},value);

            }
        }
    }

}
