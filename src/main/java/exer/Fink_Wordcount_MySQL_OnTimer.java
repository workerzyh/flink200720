package exer;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author zhengyonghong
 * @create 2020--12--16--8:33
 */
public class Fink_Wordcount_MySQL_OnTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //配置kafka信息
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put("auto.offset.reset","latest");

        //读取kafka数据并创建流
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>("testTopic", new SimpleStringSchema(), props));

        //TODO 次数存入a表
        //转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = kafkaDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0], 1);
            }
        });

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOne.keyBy(0);

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDS.sum(1);

        //写出到mysql
        DataStreamSink<Tuple2<String, Integer>> wordToMySQL = result.addSink(new MyWordToMySQLFunc());

        //TODO 预警信息输出到侧输出流
        SingleOutputStreamOperator<SensorReading> wordToSensor = kafkaDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        //分组
        KeyedStream<SensorReading, Tuple> SensorkeyedDS = wordToSensor.keyBy("id");

        //定时器处理
        SingleOutputStreamOperator<String> resultProcess = SensorkeyedDS.process(new MyOnTimerProcess(10));

        //获取侧输出流结果
        DataStream<String> sideOutput = resultProcess.getSideOutput(new OutputTag<String>("sensor_output") {
        });

        //TODO 输出侧输出流信息到mysql
        sideOutput.addSink(new SlideToMySQLFunc());

        //启动
        env.execute();


    }
    //TODO wordcount输出到mysql
    public static class MyWordToMySQLFunc extends RichSinkFunction<Tuple2<String, Integer>>{
       private Connection connection;
       private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","123456");
            preparedStatement = connection.prepareStatement("insert into a values(?,?) on duplicate key update count=?");
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            //配置SQL参数
            preparedStatement.setString(1,value.f0);
            preparedStatement.setInt(2,value.f1);
            preparedStatement.setInt(3,value.f1);

            //执行SQL
            preparedStatement.execute();

        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }

    //TODO 预警信息输出到侧输出流
    public static class MyOnTimerProcess extends KeyedProcessFunction<Tuple,SensorReading,String>{
        //定时器事件间隔
        private int maxDiff;

        public MyOnTimerProcess(int maxDiff) {
            this.maxDiff = maxDiff;
        }

        //声明温度状态
        private ValueState<Double> tempState;

        //声明定时器时间戳状态
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化温度状态
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("tempState",Double.class,Double.MIN_VALUE));

            //初始化定时器时间戳状态
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsState",Long.class));

        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //获取状态温度信息
            Double tempHis = tempState.value();

            //获取状态定时器时间戳
            Long tsHis = tsState.value();

            //获取当前传入温度
            Double curTemp = value.getTemp();

            //当传入温度大于状态温度且定时器时间戳为空，则注册定时器
            if(curTemp>tempHis && tsHis==null){
                //定时器时间戳
                long tsTimer = ctx.timerService().currentProcessingTime() + maxDiff * 1000L;

                //注册定时器
                ctx.timerService().registerProcessingTimeTimer(tsTimer);

                //更新温度和时间状态
                tempState.update(curTemp);
                tsState.update(tsTimer);

            }else if(curTemp<tempHis && tsHis!=null){  //当传入温度小于历史且定时器时间戳不为空，则删除定时器，
                //删除定时器
                ctx.timerService().deleteProcessingTimeTimer(tsHis);

                //清空定时器状态
                tsState.clear();
            }
        }
        //TODO 定时器

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //报警信息输出到侧输出流
            ctx.output(new OutputTag<String>("sensor_output"){},ctx.getCurrentKey() + "号传感器超出10s温度未下降");

            //定时器时间状态清空
            tsState.clear();

        }
    }

    //TODO 输出侧输出流信息到mysql
    public static class SlideToMySQLFunc extends RichSinkFunction<String>{
        private Connection connection;
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            //MYSQL连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useUnicode=true&characterEncodeing=UTF-8","root","123456");
            preparedStatement = connection.prepareStatement("insert into b values(?) ");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            //设置SQL参数
            preparedStatement.setString(1,value);

            //执行SQL
            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
