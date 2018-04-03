package kbasesearchengine.search;

public class PostProcessing {
    
    //TODO CODE everything about this class
    
    public boolean objectKeys;
    public boolean objectData;
    public boolean objectHighlight;
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (objectData ? 1231 : 1237);
        result = prime * result + (objectHighlight ? 1231 : 1237);
        result = prime * result + (objectKeys ? 1231 : 1237);
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
        PostProcessing other = (PostProcessing) obj;
        if (objectData != other.objectData) {
            return false;
        }
        if (objectHighlight != other.objectHighlight) {
            return false;
        }
        if (objectKeys != other.objectKeys) {
            return false;
        }
        return true;
    }
}
