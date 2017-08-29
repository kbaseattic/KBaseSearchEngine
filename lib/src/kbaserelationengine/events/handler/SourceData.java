package kbaserelationengine.events.handler;

import us.kbase.common.service.UObject;

/** A data package loaded from a particular data source.
 * @author gaprice@lbl.gov
 *
 */
public class SourceData {
    
    //TODO JAVADOC
    //TODO TEST

    private final UObject data;
    private final String name;
    
    private SourceData(final UObject data, final String name) {
        this.data = data;
        this.name = name;
    }
    
    public UObject getData() {
        return data;
    }

    public String getName() {
        return name;
    }

    public static Builder getBuilder(final UObject data, final String name) {
        return new Builder(data, name);
    }
    
    public static class Builder {
        
        private final UObject data;
        private final String name;
        
        private Builder(final UObject data, final String name) {
            //TODO NOW check for nulls & empties
            this.data = data;
            this.name = name;
        }
        
        public SourceData build() {
            return new SourceData(data, name);
        }
    }
    
}
