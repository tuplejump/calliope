package com.tuplejump.calliope;

import java.util.ArrayList;
import java.util.List;

public class Util {
    public static List<String> parseJsonAsList(String listJson) {
        String trimmedList = listJson.trim();
        String[] l1 = trimmedList.replace("[", "").replace("]", "").split(","); //substring(1, trimmedList.length() -1).split(",");
        List<String> retList = new ArrayList<String>();
        for(String str: l1){
            retList.add(str.replaceAll("\"", "").trim());
        }
        return retList;
    }
}
