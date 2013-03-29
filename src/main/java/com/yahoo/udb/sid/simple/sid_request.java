package com.yahoo.udb.sid.simple;

import com.yahoo.jcontrib.sonoraclient.UPSResponse;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.*;

/**
 * Created with IntelliJ IDEA.
 * User: pnip
 * Date: 3/27/13
 * Time: 1:03 PM
 * To change this template use File | Settings | File Templates.
 * sid_request the request as well as the response object
 */
public class sid_request {

    private final ArrayList<Map.Entry<String, UPSResponse>> upsResponses = new ArrayList<Map.Entry<String, UPSResponse>>();
    private final ArrayList<String> keys = new ArrayList<String>();
    private final Pattern regex = Pattern.compile("(.*):(.*)=?");
    private int returnCode = 200;
    private boolean shutDownNow = false;
    private String mystring;

    public sid_request(String task) throws IllegalFormatException {
        mystring = task;
        Matcher matcher = regex.matcher(task);
        while(matcher.find()) {

        }
    }

    public String mergeResponseIntoJson() {
        JSONArray jsArray = new JSONArray();
        ListIterator<Map.Entry<String,UPSResponse>> entryListIterator = null;
        for( entryListIterator = upsResponses.listIterator(); entryListIterator.hasNext();) {
            Map.Entry<String, UPSResponse> entry = (Map.Entry<String, UPSResponse>) entryListIterator.next();
            entry.getValue().getValueAsMap()
        }
    }

    public void insert(Map.Entry<String, UPSResponse> response) {
        upsResponses.add(response);
    }

    public String getString() {
        return mystring;
    }

    public int getReturnCode(){
        return returnCode;
    }

    public void setReturnCode(int ret){
        returnCode = ret;
    }

    public String getYid() {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    public List<String> getAttrs() {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    public boolean getShutDownNow() {
        return shutDownNow;
    }

    public void shutDownNow() {
        shutDownNow = true;
    }
}
