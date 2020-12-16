package sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author zhengyonghong
 * @create 2020--12--12--11:31
 */
public class Flink_sink_userdefined {
    public static void main(String[] args) throws Exception {
        //TODO 写出到mysql

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取端口数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //写出到mysql
        stream.addSink(new MySQLSink());

        //启动
        env.execute();

    }
    //TODO 涉及第三方连接时采用富函数
    public static class MySQLSink extends RichSinkFunction<String>{
        private Connection connection;
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            //创建连接，
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","123456");
            //准备编译体
            preparedStatement= connection.prepareStatement("insert into sensor_id values(?,?) on DUPLICATE KEY update temp = ?");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            //数据切割
            String[] split = value.split(",");

            //参数设置
            preparedStatement.setString(1,split[0]);
            preparedStatement.setString(2,split[2]);
            preparedStatement.setString(3,split[2]);

            //执行
            preparedStatement.execute();

        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
