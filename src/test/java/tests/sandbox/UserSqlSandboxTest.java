package tests.sandbox;

import org.commcare.core.parse.ParseUtils;
import org.javarosa.core.api.ClassNameHasher;
import org.javarosa.core.model.User;
import org.javarosa.core.util.externalizable.PrototypeFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sandbox.UserSqlSandbox;
import sqlitedb.UserDB;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the SqlSandsbox API. Just initializes and makes sure we can access at the moment.
 *
 * @author wspride
 */
public class UserSqlSandboxTest {

    private UserSqlSandbox sandbox;
    private final String username = "sandbox-test-user";

    @Before
    public void setUp() throws Exception {
        new UserDB("a", "b", null).deleteDatabaseFolder();
        sandbox = new UserSqlSandbox(new UserDB("a", "b", null));
        PrototypeFactory.setStaticHasher(new ClassNameHasher());
        ParseUtils.parseIntoSandbox(this.getClass().getClassLoader().getResourceAsStream("restores/ipm_restore.xml"), sandbox);
        sandbox = null;
    }

    @Test
    public void test() {
        sandbox = new UserSqlSandbox(new UserDB("a", "b", null));
        Assert.assertEquals(sandbox.getCaseStorage().getNumRecords(), 6);
        Assert.assertEquals(sandbox.getLedgerStorage().getNumRecords(), 3);
        Assert.assertEquals(sandbox.getUserFixtureStorage().getNumRecords(), 4);
        User loggedInUser = sandbox.getLoggedInUser();
        assertEquals(loggedInUser.getUsername(), "test");
    }

    @After
    public void tearDown(){
        new UserDB("a", "b", null).deleteDatabaseFolder();
    }
}
