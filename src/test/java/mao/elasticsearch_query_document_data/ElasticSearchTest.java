package mao.elasticsearch_query_document_data;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

/**
 * Project name(项目名称)：elasticsearch_query_document_data
 * Package(包名): mao.elasticsearch_query_document_data
 * Class(类名): ElasticSearchTest
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/5/25
 * Time(创建时间)： 13:01
 * Version(版本): 1.0
 * Description(描述)： SpringBootTest
 */

@SpringBootTest
public class ElasticSearchTest
{
    private static RestHighLevelClient client;

    @BeforeAll
    static void beforeAll()
    {
        //创建ES客户端，单例，可以交给spring管理
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    /**
     * 同步查询
     *
     * @throws IOException IOException
     */
    @Test
    void query() throws IOException
    {
        //创建请求
        GetRequest getRequest = new GetRequest("book", "2");
        //设置参数
        String[] includes = new String[]{};
        String[] excludes = new String[]{};
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);
        //设置路由
        //getRequest.routing("routing");
        //发起请求
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        //获取结果
        String id = getResponse.getId();
        long version = getResponse.getVersion();
        String sourceAsString = getResponse.getSourceAsString();
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        System.out.println("id：" + id);
        System.out.println("版本：" + version);
        System.out.println("sourceAsString：" + sourceAsString);
        System.out.println("sourceAsMap:" + sourceAsMap);
    }

    /**
     * 异步查询
     */
    @Test
    void query_async()
    {
        //创建请求
        GetRequest getRequest = new GetRequest("book", "3");
        //设置参数
        String[] includes = new String[]{};
        String[] excludes = new String[]{};
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);
        //设置路由
        //getRequest.routing("routing");
        //发起异步请求
        client.getAsync(getRequest, RequestOptions.DEFAULT, new ActionListener<>()
        {
            /**
             * 成功的回调
             *
             * @param documentFields GetResponse对象
             */
            @Override
            public void onResponse(GetResponse documentFields)
            {
                System.out.println("id：" + documentFields.getId());
                System.out.println("版本：" + documentFields.getVersion());
                System.out.println("字段：" + documentFields.getSourceAsMap());
            }

            /**
             * 失败的回调
             *
             * @param e  Exception
             */
            @Override
            public void onFailure(Exception e)
            {
                e.printStackTrace();
            }
        });
        try
        {
            //保证能收到消息
            Thread.sleep(5000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
