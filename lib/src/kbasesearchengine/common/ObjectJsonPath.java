package kbasesearchengine.common;

import java.util.Arrays;
import java.util.List;

import kbasesearchengine.parse.ObjectParseException;

public class ObjectJsonPath {
    private final String[] pathItems;
    
    public ObjectJsonPath(String fullPath) throws ObjectParseException {
        this.pathItems = parseJsonPath(fullPath);
    }
    
    public ObjectJsonPath(String[] pathItems) {
        this.pathItems = pathItems;
    }
    
    public static ObjectJsonPath path(String... pathItems) throws ObjectParseException {
        if (pathItems == null || pathItems.length == 0) {
            return new ObjectJsonPath("");
        }
        return new ObjectJsonPath(pathItems);
    }
    
    public String[] getPathItems() {
        return pathItems;
    }
    
    public static String[] parseJsonPath(String path) throws ObjectParseException {
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
                            throw new ObjectParseException("Wrong usage of ~ in json pointer path: " + 
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

    @Override
    public String toString() {
        return getPathText(pathItems);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(pathItems);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ObjectJsonPath other = (ObjectJsonPath) obj;
        if (!Arrays.equals(pathItems, other.pathItems))
            return false;
        return true;
    }
    
    
}
