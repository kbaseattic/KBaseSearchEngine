package kbaserelationengine.tools;

import com.google.common.base.Optional;

public class WorkspaceIdentifier {
    
    private final Optional<String> name;
    private final Optional<Integer> id;
    
    public WorkspaceIdentifier(final String name) {
        this.name = Optional.of(name);
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Workspace names must have at least one character");
        }
        this.id = Optional.absent();
    }
    
    public WorkspaceIdentifier(final int id) {
        this.name = null;
        this.id = Optional.of(id);
    }

    public Optional<String> getName() {
        return name;
    }

    public Optional<Integer> getId() {
        return id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
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
        WorkspaceIdentifier other = (WorkspaceIdentifier) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("WorkspaceIdentifier [name=");
        builder.append(name);
        builder.append(", id=");
        builder.append(id);
        builder.append("]");
        return builder.toString();
    }

}
