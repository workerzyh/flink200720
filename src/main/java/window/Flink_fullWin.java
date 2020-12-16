package window;

import wordcount.Flink01_Wordcount_batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author zhengyonghong
 * @create 2020--12--12--14:29
 */
public class Flink_fullWin {
    public static void main(String[] args) throws Exception {
        //TODO 全窗口函数
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //读取端口数据
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);
        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = input.flatMap(new Flink01_Wordcount_batch.MyFlatMapFunc());
        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByStream = wordToOne.keyBy(0);

        //开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> winDS = keyByStream.timeWindow(Time.seconds(10));

        //TODO 全窗口聚合函数
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = winDS.apply(new MyWondow());

        //打印
        result.print();

        //启动
        env.execute();
    }
    public static class MyWondow implements WindowFunction<Tuple2<String, Integer>,Tuple2<String, Integer>,Tuple,TimeWindow>{
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            Iterator<Tuple2<String, Integer>> iterator = input.iterator();

            //获取key值
            String key = tuple.getField(0);

            int count=0;
            //计算当前数据条数
            while (iterator.hasNext()){
                Tuple2<String, Integer> next = iterator.next();
                count = next.f1 + count;

            }

            //输出数据
            out.collect(new Tuple2<>(key,count));

        }
    }
}
