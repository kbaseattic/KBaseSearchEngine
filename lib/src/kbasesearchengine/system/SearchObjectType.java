package kbasesearchengine.system;

import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kbasesearchengine.tools.Utils;

/** A name and version of a search system data type.
 * @author gaprice@lbl.gov
 *
 */
public class SearchObjectType {

    private final static Pattern INVALID_CHARS = Pattern.compile("[^a-zA-Z\\d]+");
    public final static int MAX_TYPE_SIZE = 50; // UTF-8 chars
    
    private final String type;
    private final int version;
    
    // consider supporting unicode later
    /** Create a new type.
     * @param type the type name. Names may include numbers and lower and uppercase ASCII letters.
     * The first character must be a letter. The maximum size is {@link SearchObjectType#MAX_TYPE_SIZE}.
     * @param version the version of the type.
     * @throws IllegalArgumentException if the type name is null or whitespace or the version is
     * less than one.
     */
    public SearchObjectType(final String type, final int version) {
        Utils.notNullOrEmpty(type, "search type cannot be null or whitespace");

        try {
            if (type.getBytes("UTF-8").length > MAX_TYPE_SIZE) {
                throw new IllegalArgumentException("Search type string size must be less than " +
                        MAX_TYPE_SIZE + " UTF-8 chars");
            }
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalArgumentException("Unsupported encoding for search type string: " + ex.getMessage());
        }

        if (version < 1) {
            throw new IllegalArgumentException("search type version must be greater than zero");
        }
        this.type = type.trim();
        final Matcher m = INVALID_CHARS.matcher(this.type);
        if (m.find()) {
            throw new IllegalArgumentException(String.format(
                    "Illegal character in search type name %s: %s", this.type, m.group()));
        }
        if (!Character.isLetter(this.type.codePointAt(0))) {
            throw new IllegalArgumentException(String.format(
                    "Search type name %s must start with a letter", this.type));
        }
        this.version = version;
    }

    /** Get the type name.
     * @return the type name.
     */
    public String getType() {
        return type;
    }

    /** Get the version of the type.
     * @return the version.
     */
    public int getVersion() {
        return version;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + version;
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
        SearchObjectType other = (SearchObjectType) obj;
        if (type == null) {
            if (other.type != null) {
                return false;
            }
        } else if (!type.equals(other.type)) {
            return false;
        }
        if (version != other.version) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("SearchObjectType [type=");
        builder.append(type);
        builder.append(", version=");
        builder.append(version);
        builder.append("]");
        return builder.toString();
    }
}
