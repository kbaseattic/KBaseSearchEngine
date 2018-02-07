package kbasesearchengine.tools;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Properties;
import java.util.Set;

import com.google.common.base.Optional;

public class SearchToolsConfig {
    
    //TODO TESTS
    //TODO JAVADOC
    
    private static final String SEARCH_MONGO_HOST = "search-mongo-host";
    private static final String SEARCH_MONGO_DB = "search-mongo-db";
    private static final String SEARCH_MONGO_USER = "search-mongo-user";
    private static final String SEARCH_MONGO_PWD = "search-mongo-pwd";
    private static final String WORKER_CODES = "worker-codes";

    private static final String ELASTIC_HOST = "elastic-host";
    private static final String ELASTIC_PORT = "elastic-port";
    private static final String ELASTIC_USER = "elastic-user";
    private static final String ELASTIC_PWD = "elastic-password";
    private static final String ELASTIC_NAMESPACE = "elastic-namespace";
    private static final String TEMP_DIR = "scratch";

    private static final String WS_URL = "workspace-url";
    private static final String AUTH_URL = "auth-service-url";
    private static final String AUTH_URL_ALLOW_INSECURE = "auth-service-url-allow-insecure";
    private static final String INDEXER_TOKEN = "indexer-token";
    
    private static final String TYPES_DIR = "types-dir";
    private static final String TYPE_MAPPINGS_DIR = "type-mappings-dir";
    
    private static final String WS_MONGO_HOST = "workspace-mongo-host";
    private static final String WS_MONGO_DB = "workspace-mongo-db";
    private static final String WS_MONGO_USER = "workspace-mongo-user";
    private static final String WS_MONGO_PWD = "workspace-mongo-pwd";
    
    private static final String WS_ID_BLACKLIST = "workspace-blacklist";
    private static final String WS_TYPES_LIST = "workspace-types";

    private final String searchMongoHost;
    private final String searchMongoDB;
    private final Optional<String> searchMongoUser;
    private final Optional<char[]> searchMongoPwd;
    private final Set<String> workerCodes;
    
    private final String elasticHost;
    private final int elasticPort;
    private final Optional<String> elasticUser;
    private final Optional<char[]> elasticPassword;
    private final String elasticNamespace;
    private final String tempDir;
    
    private final URL workspaceURL;
    private final URL authURL;
    private final boolean allowInsecureAuthURL;
    private final String indexerToken;
    
    private final String typesDirectory;
    private final String typeMappingsDirectory;
    
    private final String workspaceMongoHost;
    private final String workspaceMongoDB;
    private final Optional<String> workspaceMongoUser;
    private final Optional<char[]> workspaceMongoPwd;
    private final List<WorkspaceIdentifier> workspaceBlackList;
    private final List<String> workspaceTypes;

    private SearchToolsConfig(
            final String searchMongoHost,
            final String searchMongoDB,
            final String searchMongoUser,
            String searchMongoPwd,
            final List<String> workerCodes,
            final String elasticHost,
            final int elasticPort,
            final String elasticUser,
            String elasticPassword,
            final String elasticNamespace,
            final String tempDir,
            final URL workspaceURL,
            final URL authURL,
            final boolean allowInsecureAuthURL,
            final String indexerToken,
            final String typesDirectory,
            final String typeMappingsDirectory,
            final String workspaceMongoHost,
            final String workspaceMongoDB,
            final String workspaceMongoUser,
            String workspaceMongoPwd,
            final List<WorkspaceIdentifier> workspaceBlackList,
            final List<String> workspaceTypes)
            throws SearchToolsConfigException {
        this.workspaceBlackList = Collections.unmodifiableList(workspaceBlackList);
        this.workspaceTypes = Collections.unmodifiableList(workspaceTypes);
        
        this.searchMongoHost = searchMongoHost;
        this.searchMongoDB = searchMongoDB;
        if (searchMongoUser == null ^ searchMongoPwd == null) { // xor
            searchMongoPwd = null; // gc
            throw new SearchToolsConfigException(String.format(
                    "Must provide both %s and %s params in config " +
                    " if MongoDB authentication is to be used",
                    SEARCH_MONGO_USER, SEARCH_MONGO_PWD));
        }
        this.searchMongoUser = Optional.fromNullable(searchMongoUser);
        this.searchMongoPwd = Optional.fromNullable(searchMongoPwd == null ?
                null :searchMongoPwd.toCharArray());
        searchMongoPwd = null;
        
        this.workerCodes = Collections.unmodifiableSet(new HashSet<>(workerCodes));
        
        this.elasticHost = elasticHost;
        this.elasticPort = elasticPort;
        if (elasticUser == null ^ elasticPassword == null) { // xor
            elasticPassword = null; // gc
            throw new SearchToolsConfigException(String.format(
                    "Must provide both %s and %s params in config " +
                    " if ElasticSearh authentication is to be used",
                    ELASTIC_USER, ELASTIC_PWD));
        }
        this.elasticUser = Optional.fromNullable(elasticUser);
        this.elasticPassword = Optional.fromNullable(elasticPassword == null ? null :
                elasticPassword.toCharArray());
        elasticPassword = null;
        this.elasticNamespace = elasticNamespace;
        this.tempDir = tempDir;

        this.workspaceURL = workspaceURL;
        this.authURL = authURL;
        this.allowInsecureAuthURL = allowInsecureAuthURL;
        this.indexerToken = indexerToken;
        
        this.typesDirectory = typesDirectory;
        this.typeMappingsDirectory = typeMappingsDirectory;
        
        this.workspaceMongoHost = workspaceMongoHost;
        this.workspaceMongoDB = workspaceMongoDB;
        if (workspaceMongoUser == null ^ workspaceMongoPwd == null) { // xor
            workspaceMongoPwd = null; // gc
            throw new SearchToolsConfigException(String.format(
                    "Must provide both %s and %s params in config " +
                    " if MongoDB authentication is to be used",
                    WS_MONGO_USER, WS_MONGO_PWD));
        }
        this.workspaceMongoUser = Optional.fromNullable(workspaceMongoUser);
        this.workspaceMongoPwd = Optional.fromNullable(workspaceMongoPwd == null ?
                null :workspaceMongoPwd.toCharArray());
        workspaceMongoPwd = null; //gc
    }

    public String getSearchMongoHost() {
        return searchMongoHost;
    }

    public String getSearchMongoDB() {
        return searchMongoDB;
    }

    public Optional<String> getSearchMongoUser() {
        return searchMongoUser;
    }

    public Optional<char[]> getSearchMongoPwd() {
        return searchMongoPwd;
    }
    
    public Set<String> workerCodes() {
        return workerCodes;
    }

    public String getElasticHost() {
        return elasticHost;
    }

    public int getElasticPort() {
        return elasticPort;
    }

    public Optional<String> getElasticUser() {
        return elasticUser;
    }

    public Optional<char[]> getElasticPassword() {
        return elasticPassword;
    }

    public String getElasticNamespace() {
        return elasticNamespace;
    }
    
    public String getTempDir() {
        return tempDir;
    }
    
    public URL getWorkspaceURL() {
        return workspaceURL;
    }

    public URL getAuthURL() {
        return authURL;
    }

    public boolean isAllowInsecureAuthURL() {
        return allowInsecureAuthURL;
    }

    public String getIndexerToken() {
        return indexerToken;
    }

    public String getTypesDirectory() {
        return typesDirectory;
    }

    public String getTypeMappingsDirectory() {
        return typeMappingsDirectory;
    }

    public String getWorkspaceMongoHost() {
        return workspaceMongoHost;
    }

    public String getWorkspaceMongoDB() {
        return workspaceMongoDB;
    }

    public Optional<String> getWorkspaceMongoUser() {
        return workspaceMongoUser;
    }

    public Optional<char[]> getWorkspaceMongoPwd() {
        return workspaceMongoPwd;
    }

    public List<WorkspaceIdentifier> getWorkspaceBlackList() {
        return workspaceBlackList;
    }

    public List<String> getWorkspaceTypes() {
        return workspaceTypes;
    }

    public static SearchToolsConfig from(final Properties p) throws SearchToolsConfigException {
        final Map<String, String> cfg = new HashMap<>();
        for (final Entry<Object, Object> e: p.entrySet()) {
            cfg.put((String) e.getKey(), (String) e.getValue());
        }
        return from(cfg);
    }

    public static SearchToolsConfig from(final Map<String, String> cfg)
            throws SearchToolsConfigException {
        final int esPort;
        final String esPortStr = getString(ELASTIC_PORT, cfg, true);
        try {
            esPort = Integer.parseInt(esPortStr);
        } catch (NumberFormatException e) {
            throw new SearchToolsConfigException(String.format(
                    "Invalid value for parameter %s (%s): %s",
                    ELASTIC_PORT, esPortStr, e.getMessage(), e));
        }
        return new SearchToolsConfig(
                getString(SEARCH_MONGO_HOST, cfg, true),
                getString(SEARCH_MONGO_DB, cfg, true),
                getString(SEARCH_MONGO_USER, cfg),
                getString(SEARCH_MONGO_PWD, cfg),
                getStringList(WORKER_CODES, cfg),
                getString(ELASTIC_HOST, cfg, true),
                esPort,
                getString(ELASTIC_USER, cfg),
                getString(ELASTIC_PWD, cfg),
                getString(ELASTIC_NAMESPACE, cfg, true),
                getString(TEMP_DIR, cfg, true),
                getURL(WS_URL, cfg),
                getURL(AUTH_URL, cfg),
                "true".equals(getString(AUTH_URL_ALLOW_INSECURE, cfg)),
                getString(INDEXER_TOKEN, cfg, true),
                getString(TYPES_DIR, cfg, true),
                getString(TYPE_MAPPINGS_DIR, cfg, true),
                getString(WS_MONGO_HOST, cfg, true),
                getString(WS_MONGO_DB, cfg, true),
                getString(WS_MONGO_USER, cfg),
                getString(WS_MONGO_PWD, cfg),
                getWSIDList(WS_ID_BLACKLIST, cfg),
                getStringList(WS_TYPES_LIST, cfg));
    }

    private static List<String> getStringList(
            final String configparam,
            final Map<String, String> cfg) {
        final String stringlist = cfg.get(configparam);
        final List<String> ret = new LinkedList<>();
        if (stringlist != null && !stringlist.isEmpty()) {
            for (final String s: stringlist.split(",")) {
                if (!s.trim().isEmpty()) {
                    ret.add(s);
                }
            }
        }
        return ret;
    }

    private static List<WorkspaceIdentifier> getWSIDList(
            final String configparam,
            final Map<String, String> cfg) throws SearchToolsConfigException {
        final String wsIdBlacklist = cfg.get(configparam);
        if (wsIdBlacklist != null && !wsIdBlacklist.isEmpty()) {
            final List<String> wsids = Arrays.asList(wsIdBlacklist.split(","));
            return toWorkspaceIdentifiers(wsids);
        } else {
            return new LinkedList<>();
        }
    }

    public static List<WorkspaceIdentifier> toWorkspaceIdentifiers(final List<String> wsids) {
        final List<WorkspaceIdentifier> ret = new LinkedList<>();
        for (final String id: wsids) {
            if (id != null && !id.trim().isEmpty()) {
                try {
                    ret.add(new WorkspaceIdentifier(Integer.parseInt(id.trim())));
                } catch (NumberFormatException e) {
                    ret.add(new WorkspaceIdentifier(id));
                }
            }
        }
        return ret;
    }

    // returns null if no string
    private static String getString(
            final String paramName,
            final Map<String, String> config)
            throws SearchToolsConfigException {
        return getString(paramName, config, false);
    }
    
    private static String getString(
            final String paramName,
            final Map<String, String> config,
            final boolean except)
            throws SearchToolsConfigException {
        final String s = config.get(paramName);
        if (s != null && !s.trim().isEmpty()) {
            return s.trim();
        } else if (except) {
            throw new SearchToolsConfigException(String.format(
                    "Required parameter %s not provided in configuration", paramName));
        } else {
            return null;
        }
    }
    
    private static URL getURL(final String key, final Map<String, String> cfg)
            throws SearchToolsConfigException {
        final String url = getString(key, cfg, true);
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new SearchToolsConfigException(String.format(
                    "Value %s of parameter %s is not a valid URL", url, key));
        }
    }
    
    @SuppressWarnings("serial")
    public static class SearchToolsConfigException extends Exception {
        
        public SearchToolsConfigException(final String message) {
            super(message);
        }
        
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("SearchToolsConfig [searchMongoHost=");
        builder.append(searchMongoHost);
        builder.append(", searchMongoDB=");
        builder.append(searchMongoDB);
        builder.append(", searchMongoUser=");
        builder.append(searchMongoUser);
        builder.append(", searchMongoPwd=");
        builder.append(searchMongoPwd);
        builder.append(", elasticHost=");
        builder.append(elasticHost);
        builder.append(", elasticPort=");
        builder.append(elasticPort);
        builder.append(", elasticUser=");
        builder.append(elasticUser);
        builder.append(", elasticPassword=");
        builder.append(elasticPassword);
        builder.append(", elasticNamespace=");
        builder.append(elasticNamespace);
        builder.append(", workspaceURL=");
        builder.append(workspaceURL);
        builder.append(", authURL=");
        builder.append(authURL);
        builder.append(", allowInsecureAuthURL=");
        builder.append(allowInsecureAuthURL);
        builder.append(", indexerToken=");
        builder.append(indexerToken);
        builder.append(", typesDirectory=");
        builder.append(typesDirectory);
        builder.append(", typeMappingsDirectory=");
        builder.append(typeMappingsDirectory);
        builder.append(", workspaceMongoHost=");
        builder.append(workspaceMongoHost);
        builder.append(", workspaceMongoDB=");
        builder.append(workspaceMongoDB);
        builder.append(", workspaceMongoUser=");
        builder.append(workspaceMongoUser);
        builder.append(", workspaceMongoPwd=");
        builder.append(workspaceMongoPwd);
        builder.append(", workspaceBlackList=");
        builder.append(workspaceBlackList);
        builder.append(", workspaceTypes=");
        builder.append(workspaceTypes);
        builder.append("]");
        return builder.toString();
    }

}
