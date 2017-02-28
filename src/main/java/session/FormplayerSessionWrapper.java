package session;

import org.commcare.core.interfaces.UserSandbox;
import org.commcare.core.process.CommCareInstanceInitializer;
import org.commcare.modern.session.SessionWrapper;
import org.commcare.session.CommCareSession;
import org.commcare.util.CommCarePlatform;

import java.util.Hashtable;
import java.util.Map;

/**
 * Created by willpride on 1/29/16.
 */
class FormplayerSessionWrapper extends SessionWrapper {

    private final Map<String, String> sessionData;
    private final Map<String, String> userData;

    public FormplayerSessionWrapper(CommCarePlatform platform, UserSandbox sandbox) {
        this(platform, sandbox, null, null);
    }

    public FormplayerSessionWrapper(CommCarePlatform platform, UserSandbox sandbox,
                                    Map<String, String> sessionData,
                                    Map<String, String> userData) {
        super(platform, sandbox);
        this.sessionData = sessionData;
        this.userData = userData;
    }

    public FormplayerSessionWrapper(CommCareSession session, CommCarePlatform platform, UserSandbox sandbox) {
        super(session, platform, sandbox);
        this.sessionData = null;
        this.userData = null;
    }

    @Override
    public CommCareInstanceInitializer getIIF() {
        if (initializer == null) {
            initializer = new FormplayerInstanceInitializer(this, mSandbox, mPlatform, sessionData, userData);
        }
        return initializer;
    }
}
