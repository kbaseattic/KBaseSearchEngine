package kbaserelationengine.parse;

import java.util.List;

public class ObjectJsonPath {
    private final String[] pathItems;
    
    public ObjectJsonPath(String fullPath) throws Exception {
        this.pathItems = parseJsonPath(fullPath);
    }
    
    public ObjectJsonPath(String[] pathItems) {
        this.pathItems = pathItems;
    }
    
    public String[] getPathItems() {
        return pathItems;
    }
    
    public static String[] parseJsonPath(String path) throws Exception {
        String p = path;
        if (p.startsWith("/"))
            p = p.substring(1);
        if (p.endsWith("/"))
            p = p.substring(0, p.length() - 1);
        String[] ret = p.split("/");
        for (int pos = 0; pos < ret.length; pos++) {
            if (ret[pos].indexOf('~') >= 0) {
                StringBuilder item = new StringBuilder(ret[pos]);
                int n = 0;
                int origN = 0;
                while (n < item.length()) {
                    if (item.charAt(n) == '~') {
                        char next = n + 1 < item.length() ? item.charAt(n + 1) : (char)0;
                        if (next == '1') {
                            item.setCharAt(n, '/');
                        } else if (next != '0') {
                            String errorBlock = ret[pos];
                            errorBlock = errorBlock.substring(0, origN) + "[->]" + errorBlock.substring(origN);
                            throw new Exception("Wrong usage of ~ in json pointer path: " + 
                                    path + " (" + errorBlock + ")");
                        }
                        item.deleteCharAt(n + 1);
                        origN++;
                    }
                    n++;
                    origN++;
                }
                ret[pos] = item.toString();
            }
        }
        return ret;
    }
    
    public static String getPathText(List<String> path) {
        StringBuilder ret = new StringBuilder();
        for (String item : path)
            ret.append("/").append(item.replace("~", "~0").replace("/", "~1"));
        return ret.toString();
    }

    public static String getPathText(String[] path) {
        StringBuilder ret = new StringBuilder();
        for (String item : path)
            ret.append("/").append(item.replace("~", "~0").replace("/", "~1"));
        return ret.toString();
    }
}
