package kbasesearchengine.parse;

public abstract class IdConsumer implements ValueConsumer<IdMappingRules> {
    public abstract void setPrimaryId(Object value);
    
    @Override
    public void addValue(IdMappingRules rules, Object value) {
        setPrimaryId(value);
    }
}
