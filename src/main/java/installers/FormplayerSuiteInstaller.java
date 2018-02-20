package installers;

import org.commcare.resources.model.ResourceTable;
import org.commcare.resources.model.installers.SuiteInstaller;
import org.commcare.suite.model.Suite;
import org.commcare.util.CommCarePlatform;
import org.commcare.xml.SuiteParser;
import org.javarosa.core.services.storage.IStorageUtilityIndexed;
import org.javarosa.core.util.externalizable.DeserializationException;
import org.javarosa.core.util.externalizable.ExtUtil;
import org.javarosa.core.util.externalizable.PrototypeFactory;
import parsers.FormplayerSuiteParser;
import services.FormplayerStorageFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by willpride on 12/1/16.
 */
public class FormplayerSuiteInstaller extends SuiteInstaller {

    FormplayerStorageFactory storageFactory;

    public FormplayerSuiteInstaller(){}

    public FormplayerSuiteInstaller(FormplayerStorageFactory storageFactory) {
        this.storageFactory = storageFactory;
    }

    protected SuiteParser getSuiteParser(InputStream incoming, ResourceTable table, String guid, IStorageUtilityIndexed formInstanceStorage) throws IOException {
        return new FormplayerSuiteParser(incoming, table, guid, formInstanceStorage);
    }

    @Override
    protected IStorageUtilityIndexed<Suite> storage(CommCarePlatform platform) {
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
        String asUsername = ExtUtil.nullIfEmpty(ExtUtil.readString(in));
        storageFactory = new FormplayerStorageFactory();
        storageFactory.configure(username, domain, appId, asUsername);

    }

    @Override
    public void writeExternal(DataOutputStream out) throws IOException {
        super.writeExternal(out);
        ExtUtil.writeString(out, ExtUtil.emptyIfNull(storageFactory.getUsername()));
        ExtUtil.writeString(out, ExtUtil.emptyIfNull(storageFactory.getDomain()));
        ExtUtil.writeString(out, ExtUtil.emptyIfNull(storageFactory.getAppId()));
        ExtUtil.writeString(out, ExtUtil.emptyIfNull(storageFactory.getAsUsername()));
    }
}
