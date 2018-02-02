package util;

import org.javarosa.core.services.PropertyManager;
import org.javarosa.core.services.storage.IStorageUtilityIndexed;

/**
 * Created by willpride on 3/18/17.
 */
public class FormplayerPropertyManager extends PropertyManager {

    public final static String YES = "yes";
    public final static String NO = "no";
    public final static String NONE = "none";

    public static final String ENABLE_BULK_PERFORMANCE = "cc-enable-bulk-performance";
    public static final String AUTO_PURGE_ENABLED = "cc-auto-purge";

    /**
     * Constructor for this PropertyManager
     *
     * @param properties
     */
    public FormplayerPropertyManager(IStorageUtilityIndexed properties) {
        super(properties);
    }

    private boolean doesPropertyMatch(String key, String defaultValue, String matchingValue) {
        try {
            String property = getSingularProperty(key);
            return property.equals(matchingValue);
        } catch (RuntimeException e) {
            return defaultValue.equals(matchingValue);
        }
    }

    public boolean isBulkPerformanceEnabled() {
        return doesPropertyMatch(ENABLE_BULK_PERFORMANCE, NO, YES);
    }

    public boolean isAutoPurgeEnabled() {
        return doesPropertyMatch(AUTO_PURGE_ENABLED, NO, YES);
    }
}
