package kbaserelationengine;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import us.kbase.auth.AuthToken;
import us.kbase.common.service.JsonServerMethod;
import us.kbase.common.service.JsonServerServlet;
import us.kbase.common.service.JsonServerSyslog;
import us.kbase.common.service.RpcContext;

//BEGIN_HEADER

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import kbaserelationengine.common.GUID;
import kbaserelationengine.main.LineLogger;
import kbaserelationengine.main.MainObjectProcessor;
import kbaserelationengine.search.ElasticIndexingStorage;

import us.kbase.auth.AuthConfig;
import us.kbase.auth.ConfigurableAuthService;
//END_HEADER

/**
 * <p>Original spec-file module name: KBaseRelationEngine</p>
 * <pre>
 * A KBase module: KBaseRelationEngine
 * </pre>
 */
public class KBaseRelationEngineServer extends JsonServerServlet {
    private static final long serialVersionUID = 1L;
    private static final String version = "0.0.1";
    private static final String gitUrl = "https://github.com/kbaseapps/KBaseRelationEngine";
    private static final String gitCommitHash = "e209e1a11909abaf3d2b498be49047b19e911643";

    //BEGIN_CLASS_HEADER
    private MainObjectProcessor mop = null;
    
    private static void deleteMongoDb(String mongoHost, int mongoPort, String dbName) {
        try (MongoClient mongoClient = new MongoClient(mongoHost, mongoPort)) {
            System.out.println("Deleting Mongo database: " + dbName);
            mongoClient.dropDatabase(dbName);
        }
    }
    
    private static void deleteAllElasticIndices(HttpHost esHostPort, String esUser,
            String esPassword, String prefix) throws IOException {
        ElasticIndexingStorage esStorage = new ElasticIndexingStorage(esHostPort, null);
        if (esUser != null) {
            esStorage.setEsUser(esUser);
            esStorage.setEsPassword(esPassword);
        }
        for (String indexName : esStorage.listIndeces()) {
            if (indexName.startsWith(prefix)) {
                System.out.println("Deleting Elastic index: " + indexName);
                esStorage.deleteIndex(indexName);
            }
        }
    }
    
    // can't close mongo client or the connection shuts down
    // may need a shut down listener to ensure the client shuts down, but probably unnecessary
    @SuppressWarnings("resource")
    private MongoDatabase getMongoDB(
            final String host,
            final String dbname,
            final String user,
            final String pwd) {
        final MongoClient cli;
        if (user != null && !user.trim().isEmpty()) {
            if (pwd == null || pwd.trim().isEmpty()) {
                throw new IllegalArgumentException("Must provide mongo pwd if providing user");
            }
            final List<MongoCredential> creds = Arrays.asList(MongoCredential.createCredential(
                    user, dbname, pwd.toCharArray()));
            // unclear if and when it's safe to clear the password
            cli = new MongoClient(new ServerAddress(host), creds);
        } else {
            cli = new MongoClient(new ServerAddress(host));
        }
        final MongoDatabase database = cli.getDatabase(dbname);
        return database;
    }
    
    private void quietLoggers() {
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
                .setLevel(Level.INFO);
    }
    //END_CLASS_HEADER

    public KBaseRelationEngineServer() throws Exception {
        super("KBaseRelationEngine");
        //BEGIN_CONSTRUCTOR
        quietLoggers();
        URL wsUrl = new URL(config.get("workspace-url"));
        String tokenStr = config.get("indexer-token");
        final String authURL = getAuthUrlFromConfig(config);
        final AuthConfig c = new AuthConfig();
        if ("true".equals(getAuthAllowInsecureFromConfig(config))) {
            c.withAllowInsecureURLs(true);
        }
        c.withKBaseAuthServerURL(new URL(authURL));
        ConfigurableAuthService auth = new ConfigurableAuthService(c);
        AuthToken kbaseIndexerToken = auth.validateToken(tokenStr);
        final String mongoHost = config.get("mongo-host");
        final int mongoPort = Integer.parseInt(config.get("mongo-port"));
        final String mongoDbName = config.get("mongo-database");
        final String mongoUser = config.get("mongo-user");
        final String mongoPwd = config.get("mongo-pwd");
        final MongoDatabase db = getMongoDB(
                mongoHost + ":" + mongoPort, mongoDbName, mongoUser, mongoPwd);
        String elasticHost = config.get("elastic-host");
        int elasticPort = Integer.parseInt(config.get("elastic-port"));
        String esUser = config.get("elastic-user");
        String esPassword = config.get("elastic-password");
        HttpHost esHostPort = new HttpHost(elasticHost, elasticPort);
        File typesDir = new File(config.get("types-dir"));
        File tempDir = new File(config.get("scratch"));
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        String esIndexPrefix = config.get("elastic-namespace") + ".";
        String adminsText = config.get("admins");
        Set<String> admins = new LinkedHashSet<>();
        if (adminsText != null) {
            admins.addAll(Arrays.asList(adminsText.split(",")).stream().map(String::trim).collect(
                    Collectors.toList()));
        }
        String cleanDbs = config.get("clean-dbs");
        if ("true".equals(cleanDbs)) {
            try {
                deleteMongoDb(mongoHost, mongoPort, mongoDbName);
            } catch (Exception e) {
                System.out.println("Error deleting Mongo database: " + e.getMessage());
            }
            try {
                deleteAllElasticIndices(esHostPort, esUser, esPassword, esIndexPrefix);
            } catch (Exception e) {
                System.out.println("Error deleting Elastic index: " + e.getMessage());
            }
        }
        File logFile = new File(tempDir, "log_" + System.currentTimeMillis() + ".txt");
        PrintWriter logPw = new PrintWriter(logFile);
        mop = new MainObjectProcessor(wsUrl, kbaseIndexerToken, db,
                esHostPort, esUser, esPassword, esIndexPrefix, 
                typesDir, tempDir, true, true, new LineLogger() {
                    @Override
                    public void logInfo(String line) {
                        logPw.println(line);
                        logPw.flush();
                    }
                    @Override
                    public void logError(String line) {
                        logPw.println(line);
                        logPw.flush();
                    }
                    @Override
                    public void logError(Throwable error) {
                        error.printStackTrace(logPw);
                        logPw.flush();
                    }
                    @Override
                    public void timeStat(GUID guid, long loadMs, long parseMs, long indexMs) {
                    }
                }, admins);
        //END_CONSTRUCTOR
    }

    /**
     * <p>Original spec-file function name: search_types</p>
     * <pre>
     * Search for number of objects of each type matching constrains.
     * </pre>
     * @param   params   instance of type {@link kbaserelationengine.SearchTypesInput SearchTypesInput}
     * @return   instance of type {@link kbaserelationengine.SearchTypesOutput SearchTypesOutput}
     */
    @JsonServerMethod(rpc = "KBaseRelationEngine.search_types", async=true)
    public SearchTypesOutput searchTypes(SearchTypesInput params, AuthToken authPart, RpcContext jsonRpcContext) throws Exception {
        SearchTypesOutput returnVal = null;
        //BEGIN search_types
        returnVal = mop.searchTypes(params, authPart.getUserName());
        //END search_types
        return returnVal;
    }

    /**
     * <p>Original spec-file function name: search_objects</p>
     * <pre>
     * Search for objects of particular type matching constrains.
     * </pre>
     * @param   params   instance of type {@link kbaserelationengine.SearchObjectsInput SearchObjectsInput}
     * @return   instance of type {@link kbaserelationengine.SearchObjectsOutput SearchObjectsOutput}
     */
    @JsonServerMethod(rpc = "KBaseRelationEngine.search_objects", async=true)
    public SearchObjectsOutput searchObjects(SearchObjectsInput params, AuthToken authPart, RpcContext jsonRpcContext) throws Exception {
        SearchObjectsOutput returnVal = null;
        //BEGIN search_objects
        returnVal = mop.searchObjects(params, authPart.getUserName());
        //END search_objects
        return returnVal;
    }

    /**
     * <p>Original spec-file function name: get_objects</p>
     * <pre>
     * Retrieve objects by their GUIDs.
     * </pre>
     * @param   params   instance of type {@link kbaserelationengine.GetObjectsInput GetObjectsInput}
     * @return   instance of type {@link kbaserelationengine.GetObjectsOutput GetObjectsOutput}
     */
    @JsonServerMethod(rpc = "KBaseRelationEngine.get_objects", async=true)
    public GetObjectsOutput getObjects(GetObjectsInput params, AuthToken authPart, RpcContext jsonRpcContext) throws Exception {
        GetObjectsOutput returnVal = null;
        //BEGIN get_objects
        returnVal = mop.getObjects(params, authPart.getUserName());
        //END get_objects
        return returnVal;
    }

    /**
     * <p>Original spec-file function name: list_types</p>
     * <pre>
     * List registered searchable object types.
     * </pre>
     * @param   params   instance of type {@link kbaserelationengine.ListTypesInput ListTypesInput}
     * @return   instance of type {@link kbaserelationengine.ListTypesOutput ListTypesOutput}
     */
    @JsonServerMethod(rpc = "KBaseRelationEngine.list_types", async=true)
    public ListTypesOutput listTypes(ListTypesInput params, RpcContext jsonRpcContext) throws Exception {
        ListTypesOutput returnVal = null;
        //BEGIN list_types
        returnVal = new ListTypesOutput().withTypes(mop.listTypes(params.getTypeName()));
        //END list_types
        return returnVal;
    }
    @JsonServerMethod(rpc = "KBaseRelationEngine.status")
    public Map<String, Object> status() {
        Map<String, Object> returnVal = null;
        //BEGIN_STATUS
        returnVal = new LinkedHashMap<String, Object>();
        returnVal.put("state", "OK");
        returnVal.put("message", "");
        returnVal.put("version", version);
        returnVal.put("git_url", gitUrl);
        returnVal.put("git_commit_hash", gitCommitHash);
        //END_STATUS
        return returnVal;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1) {
            new KBaseRelationEngineServer().startupServer(Integer.parseInt(args[0]));
        } else if (args.length == 3) {
            JsonServerSyslog.setStaticUseSyslog(false);
            JsonServerSyslog.setStaticMlogFile(args[1] + ".log");
            new KBaseRelationEngineServer().processRpcCall(new File(args[0]), new File(args[1]), args[2]);
        } else {
            System.out.println("Usage: <program> <server_port>");
            System.out.println("   or: <program> <context_json_file> <output_json_file> <token>");
            return;
        }
    }
}
