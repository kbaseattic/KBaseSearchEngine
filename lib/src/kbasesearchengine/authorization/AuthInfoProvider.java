package kbasesearchengine.authorization;

import java.util.Map;
import java.util.Set;
import java.io.IOException;
import kbasesearchengine.authorization.TemporaryAuth2Client.Auth2Exception;

public interface AuthInfoProvider {
    public Map<String, String> findUserDisplayNames(Set<String> userNames) throws IOException, Auth2Exception;
    public String findUserDisplayName(String userName) throws IOException, Auth2Exception;
}
