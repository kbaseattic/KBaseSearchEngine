package kbasesearchengine.search;

public class MatchValue {
    
    //TODO CODE everything about this class
    
    public Object value;
    public Integer minInt;
    public Integer maxInt;
    public Long minDate; //TODO CODE use Instant
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((maxDate == null) ? 0 : maxDate.hashCode());
        result = prime * result
                + ((maxDouble == null) ? 0 : maxDouble.hashCode());
        result = prime * result + ((maxInt == null) ? 0 : maxInt.hashCode());
        result = prime * result + ((minDate == null) ? 0 : minDate.hashCode());
        result = prime * result
                + ((minDouble == null) ? 0 : minDouble.hashCode());
        result = prime * result + ((minInt == null) ? 0 : minInt.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
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
        MatchValue other = (MatchValue) obj;
        if (maxDate == null) {
            if (other.maxDate != null) {
                return false;
            }
        } else if (!maxDate.equals(other.maxDate)) {
            return false;
        }
        if (maxDouble == null) {
            if (other.maxDouble != null) {
                return false;
            }
        } else if (!maxDouble.equals(other.maxDouble)) {
            return false;
        }
        if (maxInt == null) {
            if (other.maxInt != null) {
                return false;
            }
        } else if (!maxInt.equals(other.maxInt)) {
            return false;
        }
        if (minDate == null) {
            if (other.minDate != null) {
                return false;
            }
        } else if (!minDate.equals(other.minDate)) {
            return false;
        }
        if (minDouble == null) {
            if (other.minDouble != null) {
                return false;
            }
        } else if (!minDouble.equals(other.minDouble)) {
            return false;
        }
        if (minInt == null) {
            if (other.minInt != null) {
                return false;
            }
        } else if (!minInt.equals(other.minInt)) {
            return false;
        }
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }
}
