package window;

import wordcount.Flink01_Wordcount_batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @author zhengyonghong
 * @create 2020--12--12--14:51
 */
public class Flink_CountWin_roll {
    public static void main(String[] args) throws Exception {
        //TODO 滚动计数窗口，计算10s的wordcount
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //读取端口数据
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);
        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = input.flatMap(new Flink01_Wordcount_batch.MyFlatMapFunc());
        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByStream = wordToOne.keyBy(0);
        //开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> winDS = keyByStream.countWindow(5);
        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = winDS.sum(1);
        //打印
        result.print();
        //启动
        env.execute();
    }
}
