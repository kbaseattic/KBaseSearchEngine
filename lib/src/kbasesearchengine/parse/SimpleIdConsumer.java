package kbasesearchengine.parse;

public class SimpleIdConsumer extends IdConsumer {
    private Object primaryKey = null;
    
    public Object getPrimaryKey() {
        return primaryKey;
    }
    
    @Override
    public void setPrimaryId(Object value) {
        this.primaryKey = value;
    }
    
}
