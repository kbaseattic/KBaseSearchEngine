package kbasesearchengine.parse;

import java.util.List;
import java.util.Map;

public class ParsedObject {
    
    //TODO CODE everything about this class
    
    private final String json;
    private final Map<String, List<Object>> keywords;

    public ParsedObject(final String json, final Map<String, List<Object>> keywords) {
        this.json = json;
        this.keywords = keywords;
    }
    
    public String getJson() {
        return json;
    }

    public Map<String, List<Object>> getKeywords() {
        return keywords;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((json == null) ? 0 : json.hashCode());
        result = prime * result
                + ((keywords == null) ? 0 : keywords.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ParsedObject other = (ParsedObject) obj;
        if (json == null) {
            if (other.json != null) {
                return false;
            }
        } else if (!json.equals(other.json)) {
            return false;
        }
        if (keywords == null) {
            if (other.keywords != null) {
                return false;
            }
        } else if (!keywords.equals(other.keywords)) {
            return false;
        }
        return true;
    }
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ParsedObject [json=");
        builder.append(json);
        builder.append(", keywords=");
        builder.append(keywords);
        builder.append("]");
        return builder.toString();
    }
}
