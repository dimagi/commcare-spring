package beans.menus;

import beans.InstallRequestBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Bean for requesting an update and specifying to what version
 */
public class UpdateRequestBean extends InstallRequestBean {

    private final Log log = LogFactory.getLog(UpdateRequestBean.class);

    private static final String[] VALID_UPDATE_MODES_ARRAY = new String[]{"build", "save", "release"};
    private static final Set<String> VALID_UPDATE_MODES = new HashSet<>(Arrays.asList(VALID_UPDATE_MODES_ARRAY));

    private String updateMode; // one of build, release, save

    public String getUpdateMode() {
        return updateMode;
    }

    public void setUpdateMode (String updateMode) {
        if(!VALID_UPDATE_MODES.contains(updateMode)) {
            log.error("Update mode " + updateMode + " was not valid, defaulting to release.");
            this.updateMode = "release";
        } else {
            this.updateMode = updateMode;
        }
    }
}
