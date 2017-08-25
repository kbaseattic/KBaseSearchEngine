package kbaserelationengine.tools;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Properties;

import com.google.common.base.Optional;

public class RESKEToolsConfig {
    
    private static final String WS_MONGO_HOST = "workspace-mongo-host";
    private static final String WS_MONGO_DB = "workspace-mongo-db";
    private static final String WS_MONGO_USER = "workspace-mongo-user";
    private static final String WS_MONGO_PWD = "workspace-mongo-pwd";
    
    private static final String RESKE_MONGO_HOST = "reske-mongo-host";
    private static final String RESKE_MONGO_DB = "reske-mongo-db";
    private static final String RESKE_MONGO_USER = "reske-mongo-user";
    private static final String RESKE_MONGO_PWD = "reske-mongo-pwd";
    
    private static final String WS_ID_BLACKLIST = "workspace-id-blacklist";

    private final String reskeMongoHost;
    private final String reskeMongoDB;
    private final Optional<String> reskeMongoUser;
    private final Optional<char[]> reskeMongoPwd;
    private final String workspaceMongoHost;
    private final String workspaceMongoDB;
    private final Optional<String> workspaceMongoUser;
    private final Optional<char[]> workspaceMongoPwd;
    private final List<Integer> workspaceBlackList;

    
    private RESKEToolsConfig(
            final String reskeMongoHost,
            final String reskeMongoDB,
            final String reskeMongoUser,
            String reskeMongoPwd,
            final String workspaceMongoHost,
            final String workspaceMongoDB,
            final String workspaceMongoUser,
            String workspaceMongoPwd,
            final List<Integer> wsBlackList)
            throws RESKEToolsConfigException {
        this.workspaceBlackList = Collections.unmodifiableList(wsBlackList);
        this.reskeMongoHost = reskeMongoHost;
        this.reskeMongoDB = reskeMongoDB;
        if (reskeMongoUser == null ^ reskeMongoPwd == null) { // xor
            reskeMongoPwd = null; // gc
            throw new RESKEToolsConfigException(String.format(
                    "Must provide both %s and %s params in config " +
                    " if MongoDB authentication is to be used",
                    RESKE_MONGO_USER, RESKE_MONGO_PWD));
        }
        this.reskeMongoUser = Optional.fromNullable(reskeMongoUser);
        this.reskeMongoPwd = Optional.fromNullable(reskeMongoPwd == null ?
                null :reskeMongoPwd.toCharArray());
        reskeMongoPwd = null;
        this.workspaceMongoHost = workspaceMongoHost;
        this.workspaceMongoDB = workspaceMongoDB;
        if (workspaceMongoUser == null ^ workspaceMongoPwd == null) { // xor
            workspaceMongoPwd = null; // gc
            throw new RESKEToolsConfigException(String.format(
                    "Must provide both %s and %s params in config " +
                    " if MongoDB authentication is to be used",
                    WS_MONGO_USER, WS_MONGO_PWD));
        }
        this.workspaceMongoUser = Optional.fromNullable(workspaceMongoUser);
        this.workspaceMongoPwd = Optional.fromNullable(workspaceMongoPwd == null ?
                null :workspaceMongoPwd.toCharArray());
        workspaceMongoPwd = null; //gc
    }

    public String getReskeMongoHost() {
        return reskeMongoHost;
    }

    public String getReskeMongoDB() {
        return reskeMongoDB;
    }

    public Optional<String> getReskeMongoUser() {
        return reskeMongoUser;
    }

    public Optional<char[]> getReskeMongoPwd() {
        return reskeMongoPwd;
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

    public List<Integer> getWorkspaceBlackList() {
        return workspaceBlackList;
    }

    public static RESKEToolsConfig from(final Properties p) throws RESKEToolsConfigException {
        final Map<String, String> cfg = new HashMap<>();
        for (final Entry<Object, Object> e: p.entrySet()) {
            cfg.put((String) e.getKey(), (String) e.getValue());
        }
        return from(cfg);
    }

    public static RESKEToolsConfig from(final Map<String, String> cfg)
            throws RESKEToolsConfigException {
        return new RESKEToolsConfig(
                getString(RESKE_MONGO_HOST, cfg, true),
                getString(RESKE_MONGO_DB, cfg, true),
                getString(RESKE_MONGO_USER, cfg),
                getString(RESKE_MONGO_PWD, cfg),
                getString(WS_MONGO_HOST, cfg, true),
                getString(WS_MONGO_DB, cfg, true),
                getString(WS_MONGO_USER, cfg),
                getString(WS_MONGO_PWD, cfg),
                getIntList(WS_ID_BLACKLIST, cfg));
    }
    
    private static List<Integer> getIntList(
            final String configparam,
            final Map<String, String> cfg) throws RESKEToolsConfigException {
        final List<Integer> ret = new LinkedList<>();
        final String wsIdBlacklist = cfg.get(configparam);
        if (wsIdBlacklist != null && !wsIdBlacklist.isEmpty()) {
            for (final String id: wsIdBlacklist.split(",")) {
                try {
                    ret.add(Integer.parseInt(id.trim()));
                } catch (NumberFormatException e) {
                    throw new RESKEToolsConfigException(String.format("%s in %s is not an integer",
                            id, configparam));
                }
            }
        }
        return ret;
    }

    // returns null if no string
    private static String getString(
            final String paramName,
            final Map<String, String> config)
            throws RESKEToolsConfigException {
        return getString(paramName, config, false);
    }
    
    private static String getString(
            final String paramName,
            final Map<String, String> config,
            final boolean except)
            throws RESKEToolsConfigException {
        final String s = config.get(paramName);
        if (s != null && !s.trim().isEmpty()) {
            return s.trim();
        } else if (except) {
            throw new RESKEToolsConfigException(String.format(
                    "Required parameter %s not provided in configuration", paramName));
        } else {
            return null;
        }
    }
    
    @SuppressWarnings("serial")
    public static class RESKEToolsConfigException extends Exception {
        
        public RESKEToolsConfigException(final String message) {
            super(message);
        }
        
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("RESKEToolsConfig [reskeMongoHost=");
        builder.append(reskeMongoHost);
        builder.append(", reskeMongoDB=");
        builder.append(reskeMongoDB);
        builder.append(", reskeMongoUser=");
        builder.append(reskeMongoUser);
        builder.append(", reskeMongoPwd=");
        builder.append(reskeMongoPwd);
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
        builder.append("]");
        return builder.toString();
    }

}
