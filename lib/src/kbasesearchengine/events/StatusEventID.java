package kbasesearchengine.events;

import kbasesearchengine.tools.Utils;

/** An ID for a status event, which is an arbitrary string. The generator of the ID is responsible
 * for ensuring uniqueness.
 * @author gaprice@lbl.gov
 *
 */
public class StatusEventID {
    
    private final String id;

    /** Create a new ID.
     * @param id the ID.
     */
    public StatusEventID(final String id) {
        Utils.notNullOrEmpty(id, "id cannot be null or the empty string");
        this.id = id;
    }

    /** Get the id.
     * @return the id.
     */
    public String getId() {
        return id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
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
        StatusEventID other = (StatusEventID) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("StatusEventID [id=");
        builder.append(id);
        builder.append("]");
        return builder.toString();
    }

}
