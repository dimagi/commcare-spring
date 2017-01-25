package util;

import org.commcare.modern.util.Pair;

/**
 * Created by willpride on 2/4/16.
 */
public class StringUtils {
    public static String getFullUsername(String user, String domain, String host){
        return user + "@" + domain + "." + host;
    }

    public static String getPostUrl(String host, String domain, String appId) {
        return host + "/a/" + domain + "/receiver/" + appId + "/";
    }
}
