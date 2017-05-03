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
    private static final String gitCommitHash = "a11727fe9b5d1c9216f5d672a29ca86803086f5f";

    //BEGIN_CLASS_HEADER
    //END_CLASS_HEADER

    public KBaseRelationEngineServer() throws Exception {
        super("KBaseRelationEngine");
        //BEGIN_CONSTRUCTOR
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
     * <p>Original spec-file function name: list_type_keys</p>
     * <pre>
     * </pre>
     * @param   params   instance of type {@link kbaserelationengine.ListTypeKeysInput ListTypeKeysInput}
     * @return   instance of type {@link kbaserelationengine.ListTypeKeysOutput ListTypeKeysOutput}
     */
    @JsonServerMethod(rpc = "KBaseRelationEngine.list_type_keys", async=true)
    public ListTypeKeysOutput listTypeKeys(ListTypeKeysInput params, RpcContext jsonRpcContext) throws Exception {
        ListTypeKeysOutput returnVal = null;
        //BEGIN list_type_keys
        //END list_type_keys
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
