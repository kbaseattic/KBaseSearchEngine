package kbaserelationengine.tools;

import static kbaserelationengine.tools.Utils.nonNull;

import java.util.LinkedList;
import java.util.List;

import org.bson.Document;

import com.google.common.base.Optional;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

public class WorkspaceEventGenerator {
    
    private static final int WS_COMPATIBLE_SCHEMA = 1;
    private static final String WS_COL_CONFIG = "config";
    private static final String WS_KEY_SCHEMAVER = "schemaver";
    private static final String WS_KEY_IN_UPDATE = "inupdate";

    //TODO EVENTGEN handle data palettes: 1) remove all sharing for ws 2) pull DP 3) add share events for all DP objects. RC still possible.
    
    //TODO NOW actually use these
    private final int ws;
    private final int obj;
    private final int ver;
    
    private final MongoClient reskeClient;
    private final MongoDatabase reskeDB;
    private final MongoClient wsClient;
    private final MongoDatabase wsDB;
    
    private WorkspaceEventGenerator(
            final RESKEToolsConfig cfg,
            final int ws,
            final int obj,
            final int ver) throws EventGeneratorException {
        this.ws = ws;
        this.obj = obj;
        this.ver = ver;
        if (cfg.getReskeMongoHost().equals(cfg.getWorkspaceMongoHost())) {
            final List<MongoCredential> creds = new LinkedList<>();
            addCred(cfg.getReskeMongoDB(), cfg.getReskeMongoUser(), cfg.getReskeMongoPwd(), creds);
            addCred(cfg.getWorkspaceMongoDB(), cfg.getWorkspaceMongoUser(),
                        cfg.getWorkspaceMongoPwd(), creds);
            try {
                reskeClient = new MongoClient(new ServerAddress(cfg.getReskeMongoHost()), creds);
                wsClient = reskeClient;
            } catch (MongoException e) {
                throw convert(e, null);
            }
        } else {
            reskeClient = getClient(cfg.getReskeMongoHost(), cfg.getReskeMongoDB(),
                    cfg.getReskeMongoUser(), cfg.getReskeMongoPwd());
            wsClient = getClient(cfg.getWorkspaceMongoHost(), cfg.getWorkspaceMongoDB(),
                    cfg.getWorkspaceMongoUser(), cfg.getWorkspaceMongoPwd());
            
        }
        reskeDB = reskeClient.getDatabase(cfg.getReskeMongoDB());
        wsDB = wsClient.getDatabase(cfg.getWorkspaceMongoDB());
        checkWorkspaceSchema();
        checkReskeConnection();
    }
    
    private void checkReskeConnection() throws EventGeneratorException {
        try {
            // collection may not exist yet so this is the best we can do
            reskeDB.listCollectionNames();
        } catch (MongoException e) {
            throw convert(e, "RESKE");
        }
    }

    public void destroy() {
        reskeClient.close();
        wsClient.close();
    }

    private EventGeneratorException convert(final MongoException e, final String db) {
        return new EventGeneratorException(String.format("Error connecting to %sdatabase: ",
                db == null ? "" : db + " ") + e.getMessage(), e);
    }

    private void checkWorkspaceSchema() throws EventGeneratorException {
        try {
            final Document d = wsDB.getCollection(WS_COL_CONFIG).find().first();
            if (d == null) {
                throw new EventGeneratorException(
                        "Couldn't find config document in workspace database");
            }
            if (d.getInteger(WS_KEY_SCHEMAVER) != WS_COMPATIBLE_SCHEMA) {
                throw new EventGeneratorException(String.format(
                        "Incompatible workspace schema %s. Expected %s.",
                        d.getInteger(WS_KEY_SCHEMAVER), WS_COMPATIBLE_SCHEMA));
            }
            if (d.getBoolean(WS_KEY_IN_UPDATE)) {
                throw new EventGeneratorException("Workspace schema is mid-update.");
            }
        } catch (MongoException e) {
            throw convert(e, "workspace");
        }
    }

    private MongoClient getClient(
            final String host,
            final String db,
            final Optional<String> user,
            final Optional<char[]> pwd) throws EventGeneratorException {
        try {
            return new MongoClient(new ServerAddress(host), getCred(db, user, pwd));
        } catch (MongoException e) {
            throw convert(e, db);
        }
    }

    private List<MongoCredential> getCred(
            final String db,
            final Optional<String> user,
            final Optional<char[]> pwd) {
        final List<MongoCredential> creds = new LinkedList<>();
        addCred(db, user, pwd, creds);
        return creds;
    }

    private void addCred(
            final String db,
            final Optional<String> user,
            final Optional<char[]> pwd,
            final List<MongoCredential> creds) {
        if (user.isPresent()) {
            creds.add(MongoCredential.createCredential(user.get(), db, pwd.get()));
        }
    }

    public void generateEvents() {
        // TODO Auto-generated method stub
        
    }
    
    public static class Builder {
        
        final RESKEToolsConfig cfg;
        int ws = -1;
        int obj = -1;
        int ver = -1;
        
        public Builder(final RESKEToolsConfig cfg) {
            nonNull(cfg, "cfg");
            this.cfg = cfg;
        }
        
        public Builder withNullableRef(final String ref) throws EventGeneratorException {
            if (ref != null && !ref.isEmpty()) {
                final String[] splitref = ref.split("/");
                final int ws = processRef(ref, splitref, 0, "workspace id");
                final int obj = processRef(ref, splitref, 1, "object id");
                ver = processRef(ref, splitref, 2, "version");
                this.ws = ws; // don't leave builder in inconsistent state on exception
                this.obj = obj;
            }
            return this;
        }
        
        private int processRef(
                final String ref,
                final String[] splitref,
                final int pos,
                final String refpart)
                throws EventGeneratorException {
            if (splitref.length < pos) {
                try {
                    return Integer.parseInt(splitref[pos]);
                } catch (NumberFormatException e) {
                    throw new EventGeneratorException(String.format(
                            "Cannot parse ref %s %s into an integer", ref, refpart));
                }
            }
            return -1;
        }

        public WorkspaceEventGenerator build() throws EventGeneratorException {
            return new WorkspaceEventGenerator(cfg, ws, obj, ver);
        }
        
    }
    
    @SuppressWarnings("serial")
    public static class EventGeneratorException extends Exception {
        
        public EventGeneratorException(final String message) {
            super(message);
        }
        
        public EventGeneratorException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
    

}
