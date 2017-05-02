package kbaserelationengine.search;

public class MatchValue {
    public Object value;
    public Integer minInt;
    public Integer maxInt;
    public Long minDate;
    public Long maxDate;
    public Double minDouble;
    public Double maxDouble;
    
    public MatchValue(Object value) {
        this.value = value;
    }
    
    public MatchValue(Integer minInt, Integer maxInt) {
        this.minInt = minInt;
        this.maxInt = maxInt;
    }

    public MatchValue(Long minDate, Long maxDate) {
        this.minDate = minDate;
        this.maxDate = maxDate;
    }
    public MatchValue(Double minDouble, Double maxDouble) {
        this.minDouble = minDouble;
        this.maxDouble = maxDouble;
    }
}
