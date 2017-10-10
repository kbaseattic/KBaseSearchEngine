package kbasesearchengine.tools;

import static kbasesearchengine.tools.Utils.noNulls;
import static kbasesearchengine.tools.Utils.nonNull;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventType;
import kbasesearchengine.events.storage.StatusEventStorage;
import kbasesearchengine.system.StorageObjectType;

/** Generates events from the workspace and inserts them into the search queue.
 * 
 * Due to technical issues, interfaces directly with the workspace DB instead of going through
 * the workspace library classes, which would be preferred in general.
 * 
 * Generates events based on the search prototype event handler in the workspace, so if that
 * changes this code will likely need to change.
 * 
 * @author gaprice@lbl.gov
 *
 */
public class WorkspaceEventGenerator {
    
    private static final int WS_COMPATIBLE_SCHEMA = 1;
    private static final String WS_COL_CONFIG = "config";
    private static final String WS_COL_WORKSPACES = "workspaces";
    private static final String WS_COL_WORKSPACE_ACLS = "workspaceACLs";
    private static final String WS_COL_VERS = "workspaceObjVersions";
    private static final String WS_COL_OBJS = "workspaceObjects";
    private static final String WS_PUB_USER = "*";
    private static final int WS_PUB_PERM = 10;
    private static final String WS_KEY_WS_ID = "ws";
    private static final String WS_KEY_WS_NAME = "name";
    private static final String WS_KEY_OBJ_ID = "id";
    private static final String WS_KEY_VER = "ver";
    private static final String WS_KEY_TYPE = "type";
    private static final String WS_KEY_SAVEDATE = "savedate";
    private static final String WS_KEY_WS_ACL_ID = "id";
    private static final String WS_KEY_WS_ACL_PERM = "perm";
    private static final String WS_KEY_WS_USER = "user";
    private static final String WS_KEY_WS_DEL = "del";
    private static final String WS_KEY_OBJ_DEL = "del";
    private static final String WS_KEY_SCHEMAVER = "schemaver";
    private static final String WS_KEY_IN_UPDATE = "inupdate";
    
    //TODO EVENTGEN optimize by not pulling unneeded fields from db

    //TODO EVENTGEN handle data palettes: 1) remove all sharing for ws 2) pull DP 3) add share events for all DP objects. RC still possible.
    //TODO TEST
    
    private final int ws;
    private final int obj;
    private final int ver;
    
    private final StatusEventStorage storage;
    private final MongoDatabase wsDB;
    private final PrintStream logtarget;
    private final Set<WorkspaceIdentifier> wsBlackList;
    private List<Pattern> wsTypes;
    
    private WorkspaceEventGenerator(
            final StatusEventStorage storage,
            final MongoDatabase workspaceDatabase,
            final int ws,
            final int obj,
            final int ver,
            final PrintStream logtarget,
            final Collection<WorkspaceIdentifier> wsBlackList,
            final Collection<String> wsTypes)
            throws EventGeneratorException {
        this.ws = ws;
        this.obj = obj;
        this.ver = ver;
        this.storage = storage;
        this.wsDB = workspaceDatabase;
        this.logtarget = logtarget;
        this.wsBlackList = Collections.unmodifiableSet(new HashSet<>(wsBlackList));
        this.wsTypes = processTypes(wsTypes);
        checkWorkspaceSchema();
    }
    
    private List<Pattern> processTypes(final Collection<String> wsTypes) {
        final List<Pattern> ret = new LinkedList<>();
        for (final String t: wsTypes) {
            // always do a prefix regex so mongo can use indexes
            ret.add(Pattern.compile("^" + Pattern.quote(t.trim()))); // set up mongo regex
        }
        return ret;
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

    public void generateEvents() throws EventGeneratorException {
        if (ws > 0) {
            processWorkspace(ws);
        } else {
            try {
                // don't pull all workspaces at once to try and avoid race conditions
                final FindIterable<Document> cur = wsDB.getCollection(WS_COL_WORKSPACES)
                        .find().sort(new Document(WS_KEY_WS_ID, 1));
                for (final Document ws: cur) {
                    final int id = Math.toIntExact(ws.getLong(WS_KEY_WS_ID));
                    final String wsname = ws.getString(WS_KEY_WS_NAME);
                    if (wsBlackList.contains(new WorkspaceIdentifier(id)) ||
                            wsBlackList.contains(new WorkspaceIdentifier(wsname))) {
                        log(String.format("Skipping blacklisted workspace %s (%s)",
                                wsname, id));
                    } else if (ws.getBoolean(WS_KEY_WS_DEL)) {
                        log(String.format("Skipping deleted workspace %s (%s)", id, wsname));
                    } else {
                        processWorkspace(id);
                    }
                }
            } catch (MongoException e) {
                throw convert(e, "workspace");
            }
        }
        log("Finished processing.");
    }
    
    private void processWorkspace(final int wsid) throws EventGeneratorException {
        final boolean pub = isPub(wsid);
        final Document query = new Document(WS_KEY_WS_ID, wsid);
        if (obj > 0) {
            query.append(WS_KEY_OBJ_ID, obj);
        }
        if (ver > 0) {
            query.append(WS_KEY_VER, ver);
        }
        if (!wsTypes.isEmpty()) {
            query.append(WS_KEY_TYPE, new Document("$in", wsTypes));
        }
        final MongoCursor<Document> vercur = wsDB.getCollection(WS_COL_VERS)
                .find(query)
                .sort(new Document(WS_KEY_WS_ID, 1)
                        .append(WS_KEY_OBJ_ID, 1)
                        .append(WS_KEY_VER, -1)).iterator();

        Versions vers = new Versions(vercur, 10000);
        while (!vers.isEmpty()) {
            processVers(wsid, vers, pub);
            vers = new Versions(vercur, 10000);
        }
    }

    private void processVers(final int wsid, final Versions vers, final boolean pub)
            throws EventGeneratorException {
        final Map<Integer, Document> objects = getObjects(wsid, vers.minObjId, vers.maxObjId);
        for (final Document ver: vers.versions) {
            final int objid = Math.toIntExact(ver.getLong(WS_KEY_OBJ_ID));
            final Document obj = objects.get(objid);
            if (obj.getBoolean(WS_KEY_OBJ_DEL)) {
                final int version = ver.getInteger(WS_KEY_VER);
                log(String.format("Skipping deleted object %s/%s/%s", wsid, objid, version));
            } else {
                generateEvent(wsid, pub, ver);
            }
        }
    }

    private void generateEvent(final int wsid, final boolean pub, final Document ver)
            throws EventGeneratorException {
        final int objid = Math.toIntExact(ver.getLong(WS_KEY_OBJ_ID));
        final int vernum = ver.getInteger(WS_KEY_VER);
        final String[] typeString = ver.getString(WS_KEY_TYPE).split("-");
        final String type = typeString[0];
        final int typever = Integer.parseInt(typeString[1].split("\\.")[0]);
        try {
            storage.store(new StatusEvent(
                    null, // no mongo id
                    wsid,
                    objid + "",
                    vernum,
                    null,
                    null,
                    ver.getDate(WS_KEY_SAVEDATE).getTime(),
                    new StorageObjectType("WS", type, typever),
                    StatusEventType.NEW_VERSION,
                    pub));
        } catch (IOException e) {
            throw new EventGeneratorException("Error saving event to RESKE db: " + e.getMessage(),
                    e);
        }
        log(String.format("Generated event %s/%s/%s %s-%s", wsid, objid, vernum, type, typever));
    }

    private Map<Integer, Document> getObjects(
            final int wsid,
            final int minObjId,
            final int maxObjId)
            throws EventGeneratorException {
        final Map<Integer, Document> ret = new HashMap<>();
        try {
            final FindIterable<Document> objs = wsDB.getCollection(WS_COL_OBJS)
                    .find(new Document(WS_KEY_WS_ID, wsid)
                            .append(WS_KEY_OBJ_ID,
                                    new Document("$gte", minObjId)
                                            .append("$lte", maxObjId)));
            for (final Document obj: objs) {
                ret.put(Math.toIntExact(obj.getLong(WS_KEY_OBJ_ID)), obj);
            }
        } catch (MongoException e) {
            throw convert(e, "workspace");
        }
        return ret;
    }

    private class Versions {
        
        public final int minObjId;
        public final int maxObjId;
        public final List<Document> versions = new LinkedList<>();
        
        /* It is expected that the cursor is sorted by object id */
        public Versions(final MongoCursor<Document> vercur, final int count)
                throws EventGeneratorException {
            int minId = Integer.MAX_VALUE;
            int maxId = -1;
            try {
                int i = 0;
                while (vercur.hasNext() && i < count) {
                    final Document ver = vercur.next();
                    final int id = Math.toIntExact(ver.getLong(WS_KEY_OBJ_ID));
                    if (id < minId) {
                        minId = id;
                    }
                    if (id > maxId) {
                        maxId = id;
                    }
                    versions.add(ver);
                    i++;
                }
                minObjId = minId;
                maxObjId = maxId;
            } catch (MongoException e) {
                throw convert(e, "workspace");
            }
        }
        
        public boolean isEmpty() {
            return versions.isEmpty();
        }
    }

    private boolean isPub(final int wsid) throws EventGeneratorException {
        final boolean pub;
        try {
            final Document pubdoc = wsDB.getCollection(WS_COL_WORKSPACE_ACLS)
                    .find(new Document(WS_KEY_WS_USER, WS_PUB_USER)
                            .append(WS_KEY_WS_ACL_ID, wsid)).first();
            if (pubdoc == null) {
                pub = false;
            } else {
                pub = pubdoc.getInteger(WS_KEY_WS_ACL_PERM) == WS_PUB_PERM;
            }
        } catch (MongoException e) {
            throw convert(e, "workspace");
        }
        return pub;
    }

    private void log(final String string) {
        logtarget.println(Instant.now().toEpochMilli() + " " + string);
    }

    public static class Builder {
        
        private final StatusEventStorage storage;
        private final MongoDatabase workspaceDatabase;
        private int ws = -1;
        private int obj = -1;
        private int ver = -1;
        private PrintStream logtarget;
        private Collection<WorkspaceIdentifier> wsBlackList = new LinkedList<>();
        private Collection<String> wsTypes = new LinkedList<>();
        
        public Builder(
                final StatusEventStorage storage,
                final MongoDatabase workspaceDatabase,
                final PrintStream logtarget) {
            nonNull(storage, "storage");
            nonNull(workspaceDatabase, "workspaceDatabase");
            nonNull(logtarget, "logtarget");
            this.storage = storage;
            this.workspaceDatabase = workspaceDatabase;
            this.logtarget = logtarget;
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
            if (pos < splitref.length) {
                try {
                    return Integer.parseInt(splitref[pos]);
                } catch (NumberFormatException e) {
                    throw new EventGeneratorException(String.format(
                            "Cannot parse ref %s %s into an integer", ref, refpart));
                }
            }
            return -1;
        }

        public Builder withWorkspaceBlacklist(final Collection<WorkspaceIdentifier> wsBlackList) {
            nonNull(wsBlackList, "wsBlackList");
            noNulls(wsBlackList, "null item in wsBlackList");
            this.wsBlackList = wsBlackList;
            return this;
        }

        public Builder withWorkspaceTypes(final Collection<String> wsTypes) {
            nonNull(wsTypes, "wsTypes");
            noNulls(wsTypes, "null item in wsTypes");
            // todo check no whitespace only chars
            this.wsTypes  = wsTypes;
            return this;
        }

        public WorkspaceEventGenerator build() throws EventGeneratorException {
            return new WorkspaceEventGenerator(
                    storage, workspaceDatabase, ws, obj, ver, logtarget, wsBlackList, wsTypes);
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
