package com.macrosan.utils.essearch;

import com.macrosan.database.redis.RedisConnPool;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;

import java.util.ArrayList;
import java.util.List;

import static com.macrosan.constants.SysConstants.HEART_ETH1;
import static com.macrosan.constants.SysConstants.REDIS_NODEINFO_INDEX;

/**
 * @author pangchangya
 */
public class EsClientFactory {
    private static final Logger logger = LogManager.getLogger(EsClientFactory.class.getName());
    protected static RedisConnPool pool = RedisConnPool.getInstance();
    private String[] hosts;
    private String scheme = "http";
    private int port = 9200;
    private int maxRetryTimeout = 30000;
    private int connectTimeOut = 5000;
    private int socketTimeOut = 40000;
    private int connectionRequestTimeOut = 1000;
    private int maxConnectNum = 100;
    private int maxConnectPerRoute = 100;
    private HttpHost[] httpHosts;
    private boolean uniqueConnectTimeConfig = true;
    private boolean uniqueConnectNumConfig = true;
    private boolean enableSniff = true;
    private RestClientBuilder builder;
    private Sniffer sniffer = null;
    private RestClient restClient = null;
    private volatile RestHighLevelClient highClient;
    private static volatile EsClientFactory esClientFactory;

    private EsClientFactory() {
        List<String> listNodes = pool.getCommand(REDIS_NODEINFO_INDEX).keys("*");
        List<String> listIp = new ArrayList<>();
        listNodes.forEach(node -> {
            if (node.length() == 4) {
                String ip = pool.getCommand(REDIS_NODEINFO_INDEX).hget(node, HEART_ETH1);
                listIp.add(ip);
            }
        });
        hosts = listIp.toArray(new String[0]);
    }

    public static EsClientFactory getInstance() {
        if (esClientFactory == null) {
            synchronized (EsClientFactory.class) {
                if (esClientFactory == null) {
                    esClientFactory = new EsClientFactory();
                    esClientFactory.init();
                }
            }
        }
        return esClientFactory;
    }

    /**
     * 初始化builder
     *
     * @return RestClientBuilder
     */
    public RestClientBuilder init() {
        httpHosts = new HttpHost[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            httpHosts[i] = new HttpHost(hosts[i], port, scheme);
        }

        builder = RestClient.builder(httpHosts);
        builder.setMaxRetryTimeoutMillis(maxRetryTimeout);

        return builder;
    }

    /**
     * 获取高级api
     *
     * @return RestHighLevelClient
     */
    public RestHighLevelClient getHighLevelClient() {
        if (highClient == null) {
            synchronized (EsClientFactory.class) {
                if (highClient == null) {
                    highClient = new RestHighLevelClient(builder);
                }
            }
        }
        return highClient;
    }

    /**
     * 获取低级api
     *
     * @return RestClient
     */
    public RestClient getLowLevelClient() {

        if (uniqueConnectTimeConfig) {
            setConnectTimeOutConfig();
        }

        if (uniqueConnectNumConfig) {
            setMutiConnectConfig();
        }
        restClient = builder.build();
        //启用嗅探器
        if (enableSniff) {
            SniffOnFailureListener sniffOnFailureListener = new SniffOnFailureListener();
            sniffer = Sniffer.builder(restClient).setSniffIntervalMillis(60000).setSniffAfterFailureDelayMillis(30000).build();
            sniffOnFailureListener.setSniffer(sniffer);
            builder.setFailureListener(sniffOnFailureListener);
        }
        return restClient;
    }

    /**
     * 异步httpclient的连接延时配置
     */
    public void setConnectTimeOutConfig() {
        builder.setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setConnectTimeout(connectTimeOut);
            requestConfigBuilder.setSocketTimeout(socketTimeOut);
            requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeOut);
            return requestConfigBuilder;
        });
    }


    /**
     * 异步httpclient的连接数配置
     */
    public void setMutiConnectConfig() {
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setMaxConnTotal(maxConnectNum);
            httpClientBuilder.setMaxConnPerRoute(maxConnectPerRoute);
            return httpClientBuilder;
        });
    }

    /**
     * 关闭连接
     */
    public void close() {
//        IOUtils.closeQuietly(restClient);
//        IOUtils.closeQuietly(highClient);
    }

}
