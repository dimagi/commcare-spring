package installers;

import org.commcare.resources.model.installers.SuiteInstaller;
import org.commcare.suite.model.Suite;
import org.javarosa.core.services.storage.IStorageUtility;
import org.javarosa.core.util.externalizable.DeserializationException;
import org.javarosa.core.util.externalizable.ExtUtil;
import org.javarosa.core.util.externalizable.PrototypeFactory;
import services.FormplayerStorageFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by willpride on 12/1/16.
 */
public class FormplayerSuiteInstaller extends SuiteInstaller {

    FormplayerStorageFactory storageFactory;

    public FormplayerSuiteInstaller(){}

    public FormplayerSuiteInstaller(FormplayerStorageFactory storageFactory) {
        this.storageFactory = storageFactory;
    }

    @Override
    protected IStorageUtility<Suite> storage() {
        if (cacheStorage == null) {
            cacheStorage = storageFactory.newStorage(Suite.STORAGE_KEY, Suite.class);
        }
        return cacheStorage;
    }

    @Override
    public void readExternal(DataInputStream in, PrototypeFactory pf) throws IOException, DeserializationException {
        super.readExternal(in, pf);
        String username = ExtUtil.nullIfEmpty(ExtUtil.readString(in));
        String domain = ExtUtil.nullIfEmpty(ExtUtil.readString(in));
        String appId = ExtUtil.nullIfEmpty(ExtUtil.readString(in));
        storageFactory = new FormplayerStorageFactory();
        storageFactory.configure(username, domain, appId);

    }

    @Override
    public void writeExternal(DataOutputStream out) throws IOException {
        super.writeExternal(out);
        ExtUtil.writeString(out, ExtUtil.emptyIfNull(storageFactory.getUsername()));
        ExtUtil.writeString(out, ExtUtil.emptyIfNull(storageFactory.getDomain()));
        ExtUtil.writeString(out, ExtUtil.emptyIfNull(storageFactory.getAppId()));
    }
}
