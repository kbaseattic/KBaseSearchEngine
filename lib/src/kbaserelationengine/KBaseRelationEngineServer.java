package kbaserelationengine;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import us.kbase.common.service.JsonServerMethod;
import us.kbase.common.service.JsonServerServlet;
import us.kbase.common.service.JsonServerSyslog;

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
    private static final String gitUrl = "";
    private static final String gitCommitHash = "";

    //BEGIN_CLASS_HEADER
    //END_CLASS_HEADER

    public KBaseRelationEngineServer() throws Exception {
        super("KBaseRelationEngine");
        //BEGIN_CONSTRUCTOR
        //END_CONSTRUCTOR
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
