package kbasesearchengine.events.handler;

import com.google.common.base.Optional;

import kbasesearchengine.tools.Utils;
import us.kbase.common.service.UObject;

/** A data package loaded from a particular data source.
 * @author gaprice@lbl.gov
 *
 */
public class SourceData {
    
    private final UObject data;
    private final String name;
    private final String creator;
    private final Optional<String> copier;
    private final Optional<String> module;
    private final Optional<String> method;
    private final Optional<String> commitHash;
    private final Optional<String> version;
    
    private SourceData(
            final UObject data,
            final String name,
            final String creator,
            final Optional<String> copier,
            final Optional<String> module,
            final Optional<String> method,
            final Optional<String> commitHash,
            final Optional<String> version) {
        this.data = data;
        this.name = name;
        this.creator = creator;
        this.copier = copier;
        this.module = module;
        this.method = method;
        this.commitHash = commitHash;
        this.version = version;
    }
    
    /** Get the data.
     * @return the data.
     */
    public UObject getData() {
        return data;
    }

    /** Get the name of the data.
     * @return the data name.
     */
    public String getName() {
        return name;
    }

    /** Get the user that created the data.
     * @return the creator.
     */
    public String getCreator() {
        return creator;
    }

    /** Get the user that copied the data, if any.
     * @return the user that copied the data or absent.
     */
    public Optional<String> getCopier() {
        return copier;
    }

    /** Get the name of the module that was used to create the data, if available.
     * @return the module name or absent.
     */
    public Optional<String> getModule() {
        return module;
    }

    /** Get the method that was used to create the data, if available.
     * @return the method or absent.
     */
    public Optional<String> getMethod() {
        return method;
    }

    /** Get the commit hash of the software that was used to create the data, if available.
     * @return the commit hash or absent.
     */
    public Optional<String> getCommitHash() {
        return commitHash;
    }

    /** Get the version of the software that created the object, if available.
     * @return the version or absent.
     */
    public Optional<String> getVersion() {
        return version;
    }

    /** Get a builder for a SourceData instance.
     * @param data the data.
     * @param name the name of the data.
     * @param creator the creator of the data.
     * @return a builder.
     */
    public static Builder getBuilder(final UObject data, final String name, final String creator) {
        return new Builder(data, name, creator);
    }
    
    /** A builder for SourceData instances.
     * @author gaprice@lbl.gov
     *
     */
    public static class Builder {
        
        private final UObject data;
        private final String name;
        private final String creator;
        private Optional<String> copier = Optional.absent();
        private Optional<String> module = Optional.absent();
        private Optional<String> method = Optional.absent();
        private Optional<String> commitHash = Optional.absent();
        private Optional<String> version = Optional.absent();
        
        private Builder(final UObject data, final String name, final String creator) {
            Utils.nonNull(data, "data");
            Utils.notNullOrEmpty(name, "name cannot be null or the empty string");
            Utils.notNullOrEmpty(creator, "creator cannot be null or the empty string");
            this.data = data;
            this.name = name;
            this.creator = creator;
        }

        private Optional<String> checkNullOrEmpty(final String s) {
            if (Utils.isNullOrEmpty(s))
                return Optional.absent();
            else {
                return Optional.of(s);
            }
        }
        
        /** Add the name of the user that copied the data.
         * @param copier the user that copied the data.
         * @return this builder.
         */
        public Builder withNullableCopier(final String copier) {
            this.copier = checkNullOrEmpty(copier);
            return this;
        }
        
        /** Add the software module that was used to create the data.
         * @param module the software module.
         * @return this builder.
         */
        public Builder withNullableModule(final String module) {
            this.module = checkNullOrEmpty(module);
            return this;
        }
        
        /** Add the software method that was used to create the data.
         * @param method the software method.
         * @return this builder.
         */
        public Builder withNullableMethod(final String method) {
            this.method = checkNullOrEmpty(method);
            return this;
        }
        
        /** Add the commit hash of the software that was used to create this data.
         * @param commit the commit hash.
         * @return this builder.
         */
        public Builder withNullableCommitHash(final String commit) {
            this.commitHash = checkNullOrEmpty(commit);
            return this;
        }
        
        /** Add the version of the software that was used to create this data.
         * @param version the software version.
         * @return this builder.
         */
        public Builder withNullableVersion(final String version) {
            this.version = checkNullOrEmpty(version);
            return this;
        }
        
        /** Build the SourceData instance.
         * @return the SourceData.
         */
        public SourceData build() {
            return new SourceData(data, name, creator, copier, module, method, commitHash,
                    version);
        }
    }
    
}
