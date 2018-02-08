package kbasesearchengine.search;

import java.util.List;

public class PostProcessing {
    
    //TODO CODE everything about this class
    
    public boolean objectInfo;
    public boolean objectKeys;
    public boolean objectData;
    public boolean objectHighlight;
    public List<String> objectDataIncludes;
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (objectData ? 1231 : 1237);
        result = prime * result + ((objectDataIncludes == null) ? 0
                : objectDataIncludes.hashCode());
        result = prime * result + (objectInfo ? 1231 : 1237);
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
        if (objectDataIncludes == null) {
            if (other.objectDataIncludes != null) {
                return false;
            }
        } else if (!objectDataIncludes.equals(other.objectDataIncludes)) {
            return false;
        }
        if (objectInfo != other.objectInfo) {
            return false;
        }
        if (objectKeys != other.objectKeys) {
            return false;
        }
        return true;
    }
}
