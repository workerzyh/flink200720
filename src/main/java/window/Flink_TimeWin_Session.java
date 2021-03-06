package window;

import wordcount.Flink01_Wordcount_batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author zhengyonghong
 * @create 2020--12--12--14:59
 */
public class Flink_TimeWin_Session {
    public static void main(String[] args) throws Exception {
        //TODO 滚动时间窗口，计算10s的wordcount
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //读取端口数据
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);
        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = input.flatMap(new Flink01_Wordcount_batch.MyFlatMapFunc());
        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByStream = wordToOne.keyBy(0);

        //TODO 会话开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> winDS = keyByStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = winDS.sum(1);
        //打印
        result.print();
        //启动
        env.execute();
    }
}
