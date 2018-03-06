package kbasesearchengine.search;

import com.google.common.base.Optional;

import kbasesearchengine.tools.Utils;


/** A rule that specifies a field to sort on and a sort direction. Many rules can be combined into
 * an ordered collection to specify a total sort order over multiple fields.
 * 
 * One of either a key property or a standard property (see below) must be specified.
 * @author gaprice@lbl.gov
 *
 */
public class SortingRule {
    
    //TODO CODE make an enum of valid standard property field names and check against input
    
    //TODO CODE key property is a pretty vague term for what is really an object field.
    private final Optional<String> keyProperty;
    private final Optional<String> standardProperty;
    private final boolean ascending;
    
    private SortingRule(
            final String keyProperty,
            final String standardProperty,
            final boolean ascending) {
        this.keyProperty = Optional.fromNullable(keyProperty);
        this.standardProperty = Optional.fromNullable(standardProperty);
        this.ascending = ascending;
    }

    /** Get the field name for a key property to sort on, if any. A key property is synonymous
     * with an object field - e.g. the specified property is a field inside the object's data,
     * rather than general information that all objects possess (such as name, timestamp, etc.).
     * @return the key property or absent.
     */
    public Optional<String> getKeyProperty() {
        return keyProperty;
    }

    /** Get the name of a standard property to sort on, if any. A standard property is a property
     * that all objects possess, such as name, timestamp, etc.
     * @return the standard property or absent.
     */
    public Optional<String> getStandardProperty() {
        return standardProperty;
    }

    /** Returns true if the sort should be ascending for this field, or false for descending.
     * @return true if the sort is an ascending sort.
     */
    public boolean isAscending() {
        return ascending;
    }
    
    /** Returns true if the property for this sorting rule is a key property, not a standard
     * property.
     * @return true if the property is a key property.
     */
    public boolean isKeyProperty() {
        return keyProperty.isPresent();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (ascending ? 1231 : 1237);
        result = prime * result
                + ((keyProperty == null) ? 0 : keyProperty.hashCode());
        result = prime * result + ((standardProperty == null) ? 0
                : standardProperty.hashCode());
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
        SortingRule other = (SortingRule) obj;
        if (ascending != other.ascending) {
            return false;
        }
        if (keyProperty == null) {
            if (other.keyProperty != null) {
                return false;
            }
        } else if (!keyProperty.equals(other.keyProperty)) {
            return false;
        }
        if (standardProperty == null) {
            if (other.standardProperty != null) {
                return false;
            }
        } else if (!standardProperty.equals(other.standardProperty)) {
            return false;
        }
        return true;
    }
    
    /** Get a builder for a {@link SortingRule} with a key property.
     * @param keyProperty the key property.
     * @return a new builder.
     */
    public static Builder getKeyPropertyBuilder(final String keyProperty) {
        return new Builder(keyProperty, true);
    }
    
    /** Get a builder for a {@link SortingRule} with a standard property.
     * @param property the standard property.
     * @return a new builder.
     */
    public static Builder getStandardPropertyBuilder(final String property) {
        return new Builder(property, false);
    }
    
    /** A builder for {@link SortingRule}s.
     * @author gaprice@lbl.gov
     *
     */
    public static class Builder {
        
        private final String keyProperty;
        private final String standardProperty;
        private boolean ascending = true;
        
        private Builder(final String property, final boolean isKeyProperty) {
            Utils.notNullOrEmpty(property,
                    "The key property or standard property cannot be null or whitespace only");
            if (isKeyProperty) {
                keyProperty = property;
                standardProperty = null;
            } else {
                keyProperty = null;
                standardProperty = property;
            }
        }
        
        /** Specify whether the sort for this field is ascending (true) or descending(false).
         * Null values are ignored.
         * @param isAscending true if the sort is ascending.
         * @return this builder.
         */
        public Builder withNullableIsAscending(final Boolean isAscending) {
            if (isAscending != null) {
                ascending = isAscending;
            }
            return this;
        }
        
        /** Build the {@link SortingRule}.
         * @return a new {@link SortingRule}.
         */
        public SortingRule build() {
            return new SortingRule(keyProperty, standardProperty, ascending);
        }
    }
    
}
