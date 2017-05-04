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

import java.io.PrintWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;

import kbaserelationengine.main.LineLogger;
import kbaserelationengine.main.MainObjectProcessor;
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
    private static final String gitCommitHash = "3df38a259733db37d4acc4bdfcf8d41492ab9a57";

    //BEGIN_CLASS_HEADER
    private MainObjectProcessor mop = null;
    
    //END_CLASS_HEADER

    public KBaseRelationEngineServer() throws Exception {
        super("KBaseRelationEngine");
        //BEGIN_CONSTRUCTOR
        URL wsUrl = new URL(config.get("workspace-url"));
        String tokenStr = config.get("indexer-token");
        AuthToken kbaseIndexerToken = getAuth(config).validateToken(tokenStr);
        String mongoHost = config.get("mongo-host");
        int mongoPort = Integer.parseInt(config.get("mongo-port"));
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
        String mongoDbName = config.get("mongo-database");
        String esIndexPrefix = config.get("elastic-namespace") + ".";
        String adminsText = config.get("admins");
        Set<String> admins = new LinkedHashSet<>();
        if (adminsText != null) {
            admins.addAll(Arrays.asList(adminsText.split(",")).stream().map(String::trim).collect(
                    Collectors.toList()));
        }
        File logFile = new File(tempDir, "log_" + System.currentTimeMillis() + ".txt");
        PrintWriter logPw = new PrintWriter(logFile);
        mop = new MainObjectProcessor(wsUrl, kbaseIndexerToken, mongoHost,
                mongoPort, mongoDbName, esHostPort, esUser, esPassword, esIndexPrefix, 
                typesDir, tempDir, true, new LineLogger() {
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
                }, admins);
        //END_CONSTRUCTOR
    }

    /**
     * <p>Original spec-file function name: add_workspace_to_index</p>
     * <pre>
     * This operation means that given workspace will be shared with
     * system indexing user with write access. User calling this
     * function should be owner of this workspace.
     * </pre>
     * @param   params   instance of type {@link kbaserelationengine.AddWorkspaceToIndexInput AddWorkspaceToIndexInput}
     */
    @JsonServerMethod(rpc = "KBaseRelationEngine.add_workspace_to_index", async=true)
    public void addWorkspaceToIndex(AddWorkspaceToIndexInput params, AuthToken authPart, RpcContext jsonRpcContext) throws Exception {
        //BEGIN add_workspace_to_index
        String wsNameOrId = params.getWsName() != null ? params.getWsName() :
            String.valueOf(params.getWsId());
        mop.addWorkspaceToIndex(wsNameOrId, authPart);
        //END add_workspace_to_index
    }

    /**
     * <p>Original spec-file function name: search_types</p>
     * <pre>
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
     * </pre>
     * @param   params   instance of type {@link kbaserelationengine.SearchObjectsInput SearchObjectsInput}
     * @return   instance of type {@link kbaserelationengine.SearchObjectsOutput SearchObjectsOutput}
     */
    @JsonServerMethod(rpc = "KBaseRelationEngine.search_objects", async=true)
    public SearchObjectsOutput searchObjects(SearchObjectsInput params, AuthToken authPart, RpcContext jsonRpcContext) throws Exception {
        SearchObjectsOutput returnVal = null;
        //BEGIN search_objects
        //END search_objects
        return returnVal;
    }

    /**
     * <p>Original spec-file function name: get_objects</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link kbaserelationengine.GetObjectsInput GetObjectsInput}
     * @return   instance of type {@link kbaserelationengine.GetObjectsOutput GetObjectsOutput}
     */
    @JsonServerMethod(rpc = "KBaseRelationEngine.get_objects", async=true)
    public GetObjectsOutput getObjects(GetObjectsInput params, AuthToken authPart, RpcContext jsonRpcContext) throws Exception {
        GetObjectsOutput returnVal = null;
        //BEGIN get_objects
        //END get_objects
        return returnVal;
    }

    /**
     * <p>Original spec-file function name: list_types</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link kbaserelationengine.ListTypesInput ListTypesInput}
     * @return   instance of type {@link kbaserelationengine.ListTypesOutput ListTypesOutput}
     */
    @JsonServerMethod(rpc = "KBaseRelationEngine.list_types", async=true)
    public ListTypesOutput listTypes(ListTypesInput params, RpcContext jsonRpcContext) throws Exception {
        ListTypesOutput returnVal = null;
        //BEGIN list_types
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
