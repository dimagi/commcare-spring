package hq;

import beans.CaseBean;
import org.commcare.api.persistence.SqlSandboxUtils;
import org.commcare.api.persistence.SqliteIndexedStorageUtility;
import org.commcare.api.persistence.UserSqlSandbox;
import org.commcare.cases.model.Case;
import org.commcare.core.sandbox.SandboxUtils;
import org.commcare.modern.parse.ParseUtilsHelper;
import org.javarosa.core.api.ClassNameHasher;
import org.javarosa.core.model.User;
import org.javarosa.core.model.condition.EvaluationContext;
import org.javarosa.core.services.storage.IStorageIterator;
import org.javarosa.core.util.externalizable.PrototypeFactory;
import org.javarosa.xml.util.InvalidStructureException;
import org.javarosa.xml.util.UnfullfilledRequirementsException;
import org.javarosa.xpath.XPathParseTool;
import org.javarosa.xpath.expr.FunctionUtils;
import org.xmlpull.v1.XmlPullParserException;
import services.RestoreFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created by willpride on 1/7/16.
 */
public class CaseAPIs {

    public static UserSqlSandbox forceRestore(RestoreFactory restoreFactory) throws Exception {
        SqlSandboxUtils.deleteDatabaseFolder(restoreFactory.getDbFile());
        return restoreIfNotExists(restoreFactory, true);
    }

    public static UserSqlSandbox restoreIfNotExists(RestoreFactory restoreFactory, boolean overwriteCache) throws Exception{
        if (restoreFactory.isRestoreXmlExpired()) {
            SqlSandboxUtils.deleteDatabaseFolder(restoreFactory.getDbFile());
        }
        File db = new File(restoreFactory.getDbFile());
        if(db.exists()){
            return restoreFactory.getSqlSandbox();
        } else{
            db.getParentFile().mkdirs();
            String xml = restoreFactory.getRestoreXml(overwriteCache);
            return restoreUser(restoreFactory.getWrappedUsername(), restoreFactory.getDbPath(), xml);
        }
    }

    public static UserSqlSandbox restoreIfNotExists(String username, String asUsername, String domain, String xml) throws Exception {
        // This is a shitty hack to allow serialized sessions to use the RestoreFactory path methods.
        // We need a refactor of the entire infrastructure
        RestoreFactory restoreFactory = new RestoreFactory();
        restoreFactory.setDomain(domain);
        restoreFactory.setUsername(username);
        restoreFactory.setAsUsername(asUsername);
        restoreFactory.setCachedRestore(xml);
        return restoreIfNotExists(restoreFactory, false);
    }

    public static CaseBean getFullCase(String caseId, SqliteIndexedStorageUtility<Case> caseStorage){
        Case cCase = caseStorage.getRecordForValue("case-id", caseId);
        return new CaseBean(cCase);
    }

    private static UserSqlSandbox restoreUser(String username, String path, String restorePayload) throws
            UnfullfilledRequirementsException, InvalidStructureException, IOException, XmlPullParserException {
        UserSqlSandbox mSandbox = new UserSqlSandbox(username, path);
        PrototypeFactory.setStaticHasher(new ClassNameHasher());
        ParseUtilsHelper.parseXMLIntoSandbox(restorePayload, mSandbox);
        // initialize our sandbox's logged in user
        for (IStorageIterator<User> iterator = mSandbox.getUserStorage().iterate(); iterator.hasMore(); ) {
            User u = iterator.nextRecord();
            if (username.equalsIgnoreCase(u.getUsername())) {
                mSandbox.setLoggedInUser(u);
            }
        }
        return mSandbox;
    }
}
