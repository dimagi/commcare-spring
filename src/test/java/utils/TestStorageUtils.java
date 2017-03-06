package utils;

import application.SQLiteProperties;
import org.commcare.api.persistence.UserSqlSandbox;
import org.commcare.test.utilities.CaseTestUtils;
import org.javarosa.core.model.condition.EvaluationContext;
import org.javarosa.core.model.instance.DataInstance;
import org.javarosa.core.model.instance.ExternalDataInstance;
import org.javarosa.core.model.instance.TreeReference;
import session.FormplayerInstanceInitializer;

import java.util.Hashtable;

/**
 * Created by willpride on 2/21/17.
 */
public class TestStorageUtils {
    /**
     * @return An evaluation context which is capable of evaluating against
     * the connected storage instances: casedb is the only one supported for now
     */
    public static EvaluationContext getEvaluationContextWithoutSession() {
        UserSqlSandbox sandbox = new UserSqlSandbox("synctestuser", SQLiteProperties.getDataDir() + "synctestdomain");
        FormplayerInstanceInitializer iif = new FormplayerInstanceInitializer(sandbox);
        return buildEvaluationContext(iif);
    }

    private static EvaluationContext buildEvaluationContext(FormplayerInstanceInitializer iif) {
        ExternalDataInstance edi = new ExternalDataInstance(CaseTestUtils.CASE_INSTANCE, "casedb");
        DataInstance specializedDataInstance = edi.initialize(iif, "casedb");

        ExternalDataInstance ledgerDataInstanceRaw = new ExternalDataInstance(CaseTestUtils.LEDGER_INSTANCE, "ledgerdb");
        DataInstance ledgerDataInstance = ledgerDataInstanceRaw.initialize(iif, "ledger");

        Hashtable<String, DataInstance> formInstances = new Hashtable<>();
        formInstances.put("casedb", specializedDataInstance);
        formInstances.put("ledger", ledgerDataInstance);

        return new EvaluationContext(new EvaluationContext(null), formInstances, TreeReference.rootRef());
    }
}
