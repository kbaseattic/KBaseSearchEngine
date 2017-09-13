package kbasesearchengine.tools;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Properties;

import com.google.common.base.Optional;

public class SearchToolsConfig {
    
    private static final String WS_MONGO_HOST = "workspace-mongo-host";
    private static final String WS_MONGO_DB = "workspace-mongo-db";
    private static final String WS_MONGO_USER = "workspace-mongo-user";
    private static final String WS_MONGO_PWD = "workspace-mongo-pwd";
    
    private static final String SEARCH_MONGO_HOST = "search-mongo-host";
    private static final String SEARCH_MONGO_DB = "search-mongo-db";
    private static final String SEARCH_MONGO_USER = "search-mongo-user";
    private static final String SEARCH_MONGO_PWD = "search-mongo-pwd";
    
    private static final String WS_ID_BLACKLIST = "workspace-blacklist";
    private static final String WS_TYPES_LIST = "workspace-types";

    private final String searchMongoHost;
    private final String searchMongoDB;
    private final Optional<String> searchMongoUser;
    private final Optional<char[]> searchMongoPwd;
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
        return new SearchToolsConfig(
                getString(SEARCH_MONGO_HOST, cfg, true),
                getString(SEARCH_MONGO_DB, cfg, true),
                getString(SEARCH_MONGO_USER, cfg),
                getString(SEARCH_MONGO_PWD, cfg),
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
