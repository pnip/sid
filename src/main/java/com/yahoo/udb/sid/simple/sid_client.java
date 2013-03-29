package com.yahoo.udb.sid.simple;

import com.yahoo.jcontrib.sonoraclient.UPSClient;
import com.yahoo.jcontrib.sonoraclient.UPSClientConfig;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.client.HttpClient;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: pnip
 * Date: 3/27/13
 * Time: 11:35 AM
 * To change this template use File | Settings | File Templates.
 */
public class sid_client {

    // put ArrayBlockingQueue here to effect a logical time to do trimming
    // or reporting back result to caller.
    private ExecutorService pool = null;

    public static BlockingQueue<Future<sid_request>> sid_futures = null;

    public static ConcurrentHashMap<String, UPSClient> clientMap = null;


    // dont know what this is but it came from UPSClient
    // hopefully this is fixed
    private String keySchema = "[{\"name\":\"login\",\"type\":\"string\", \"metadata\" : {\"delimeter\" : \"\u0001\"}}," +
            "{\"name\":\"Gender\",\"type\":\"string\" , \"metadata\" : {}}," +
            "{\"name\" : \"ym_mail_sh\", \"type\" : \"map\", \"metadata\" : {\"keyend\" : \"\u0001\", \"entryend\" : \"\u0002\"}}" +
            "]";

    public sid_client(int parallelism, int trimThreshold) {
        sid_futures = new ArrayBlockingQueue<Future<sid_request>>(trimThreshold);
        clientMap = new ConcurrentHashMap<String, UPSClient>();

        pool =  Executors.newFixedThreadPool(parallelism);
    }

    // not sure if this can call within the constructor
    // as well. Since it might get optimized(rearranged) to call first
    // even before the array is even created so...
    public void startTrimmer() {
        pool.submit(new sid_callables.request_trimmer());
    }

    public void buildClientMap() {

        String path = new File("/home/y/conf/sidclient/config.xml").exists() ?
                "/home/y/conf/sidclient/config.xml" :
                "/Users/pnip/SID/sid/sid/src/main/resources/config.xml";
        try {
            Properties prop = new Properties();
            prop.loadFromXML(new FileInputStream(path));

            for(int i=0;;i++) {
                if( prop.containsKey("SonoraHost" + i) ) {
                    UPSClientConfig upsConfig = new UPSClientConfig();
                    upsConfig.setUpsHostName(prop.getProperty("SonoraHost" + i));
                    upsConfig.setUpsHostPort(Integer.getInteger(prop.getProperty("SonoraHost" + i + ".port")));
                    upsConfig.setUpsProtocolVersion(prop.getProperty("SonoraHost" + i + ".proto"));
                    upsConfig.setUpsYcaAppId(prop.getProperty("SonoraHost" + i + ".yca"));
                    HttpClient client = new DefaultHttpClient();
                    clientMap.putIfAbsent("SonoraHost" + i, new UPSClient(upsConfig, client, keySchema));
                } else
                    break;
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

    }

    public String getSchema() {
        return keySchema;
    }

    public void setSchema(String schema) {
        keySchema = schema;
    }

    public void shutDown() {
        // put a sentinel to indicate to the trimmer
        // to shutdown;
        try {
            sid_futures.put(new Future<sid_request>() {
                @Override
                public boolean cancel(boolean b) {
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public boolean isDone() {
                    return true;
                }

                @Override
                public sid_request get() throws InterruptedException, ExecutionException {
                    sid_request shutDownRequest = new sid_request("dummy");
                    shutDownRequest.shutDownNow();
                    return shutDownRequest;
                }

                @Override
                public sid_request get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
                    sid_request shutDownRequest = new sid_request("dummy");
                    shutDownRequest.shutDownNow();
                    return shutDownRequest;                }
            });
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        pool.shutdown();
    }

    public void putInQueue(String task) throws InterruptedException {
        sid_futures.put(pool.submit(new sid_callables.request_worker(new sid_request(task))));
    }


    public static void main(String[] args) {

        sid_client client = new sid_client(3,10000);

        client.startTrimmer();
        client.buildClientMap();

        for (String strRequest: args) {
            boolean retry = false;
            do {
                try {
                    client.putInQueue(strRequest);
                    retry = false;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (RejectedExecutionException e) {
                    retry = true;
                }
            } while(retry);
        }

        client.shutDown();
        System.out.println("finish");
    }


}
