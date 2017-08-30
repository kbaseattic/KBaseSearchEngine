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
    private final String creator;
    private final String copier;
    private final String module;
    private final String method;
    private final String commitHash;
    private final String version;
    
    private SourceData(
            final UObject data,
            final String name,
            final String creator,
            final String copier,
            final String module,
            final String method,
            final String commitHash,
            final String version) {
        this.data = data;
        this.name = name;
        this.creator = creator;
        this.copier = copier;
        this.module = module;
        this.method = method;
        this.commitHash = commitHash;
        this.version = version;
    }
    
    public UObject getData() {
        return data;
    }

    /** Get the name of the object.
     * @return
     */
    public String getName() {
        return name;
    }

    /** Get the user that created the object.
     * @return
     */
    public String getCreator() {
        return creator;
    }

    /** Get the user that copied the object, if any.
     * @return
     */
    public String getCopier() {
        return copier;
    }

    public String getModule() {
        return module;
    }

    public String getMethod() {
        return method;
    }

    public String getCommitHash() {
        return commitHash;
    }

    /** Get the version of the software that created the object.
     * @return
     */
    public String getVersion() {
        return version;
    }

    public static Builder getBuilder(final UObject data, final String name) {
        return new Builder(data, name);
    }
    
    public static class Builder {
        
        private final UObject data;
        private final String name;
        private String creator;
        private String copier;
        private String module;
        private String method;
        private String commitHash;
        private String version;
        
        private Builder(final UObject data, final String name) {
            //TODO NOW check for nulls & empties
            this.data = data;
            this.name = name;
        }
        
        public Builder withNullableCreator(final String creator) {
            this.creator = creator;
            return this;
        }
        
        public Builder withNullableCopier(final String copier) {
            this.copier = copier;
            return this;
        }
        
        public Builder withNullableModule(final String module) {
            this.module = module;
            return this;
        }
        
        public Builder withNullableMethod(final String method) {
            this.method = method;
            return this;
        }
        
        public Builder withNullableCommitHash(final String commit) {
            this.commitHash = commit;
            return this;
        }
        
        public Builder withNullableVersion(final String version) {
            this.version = version;
            return this;
        }
        
        public SourceData build() {
            return new SourceData(data, name, creator, copier, module, method, commitHash,
                    version);
        }
    }
    
}
