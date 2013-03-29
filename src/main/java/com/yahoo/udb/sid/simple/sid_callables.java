package com.yahoo.udb.sid.simple;

import com.yahoo.jcontrib.sonoraclient.*;
import com.yahoo.jcontrib.sonoraclient.internal.datatype.AuthType;
import com.yahoo.jcontrib.sonoraclient.internal.datatype.CredStore;
import com.yahoo.jcontrib.sonoraclient.internal.datatype.FamilyVersion;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: pnip
 * Date: 3/27/13
 * Time: 11:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class sid_callables {

    public static class request_trimmer implements Runnable {

        @Override
        public void run() {
            boolean shutdown = false;
            // this method will never end until a shutdown
            // sentinal is sent and the queue size drop to 0
            while(true) {
                try {
                    if((sid_client.sid_futures.size() == 0) & shutdown) {
                        break;
                    }
                    Future<sid_request> sidRequestFuture = sid_client.sid_futures.take();
                    if (sidRequestFuture.isDone() | sidRequestFuture.isCancelled()) {
                        sid_request sidRequest = sidRequestFuture.get();
                        if(sidRequest.getShutDownNow()) {
                            shutdown = true;
                            continue;
                        }
                        System.out.println("INsided exception What is my value " + sidRequest.getString());
                    } else {
                        // if it is not done put it back in the queue
                        sid_client.sid_futures.put(sidRequestFuture);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                } catch (ExecutionException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
    }

    public static class request_worker implements Callable<sid_request> {
        private sid_request sidRequest = null;

        public request_worker(sid_request sid) {
            sidRequest = sid;
        }

        @Override
        public sid_request call() throws Exception {

            CredStore credStore = new CredStore(AuthType.YID, sidRequest.getYid());

            List<UserAttribute> attrs = new ArrayList<UserAttribute>();
            attrs.add(new UserAttribute("mail_imap", FamilyVersion.V1, sidRequest.getAttrs()));

            for(Map.Entry<String, UPSClient> entry : sid_client.clientMap.entrySet()) {
                GetUserDataRequest request = entry.getValue().getUserDataRequest(credStore, attrs);
                // just insert as quickly as possible and then leave the processing to
                // the trimmer
                sidRequest.insert(new AbstractMap.SimpleEntry<String, UPSResponse>(entry.getKey(),entry.getValue().handleRequest(request)));
            }

            Thread.sleep(1000);
            sidRequest.setReturnCode(500);
            return sidRequest;
        }
    }

}
