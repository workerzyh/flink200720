package sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhengyonghong
 * @create 2020--12--12--10:20
 */
public class Flink_sink_ES {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取端口数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //写入ES
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));

        ElasticsearchSink.Builder<String> builder = new ElasticsearchSink.Builder<>(httpHosts, new MyElastic());
        //设置刷写时机
        builder.setBulkFlushMaxActions(1);

        //创建es
        ElasticsearchSink<String> ElasticBuild = builder.build();

        //写出到ES
        stream.addSink(ElasticBuild);

        //启动
        env.execute();

    }
    public static class MyElastic implements ElasticsearchSinkFunction<String>{

        @Override
        public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
            String[] split = element.split(",");
            Map<String, String> map = new HashMap<>();
            map.put(split[0], split[2]);

            IndexRequest source = Requests.indexRequest()
                    .index("sensor")
                    .type("_doc")
                    .source(map);

            indexer.add(source);
        }
    }
}
