package tests;

import org.javarosa.core.model.condition.EvaluationContext;
import org.javarosa.xpath.XPathParseTool;
import org.javarosa.xpath.expr.FunctionUtils;
import org.javarosa.xpath.expr.XPathExpression;
import org.javarosa.xpath.parser.XPathSyntaxException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import utils.TestContext;
import utils.TestStorageUtils;

import static junit.framework.Assert.assertEquals;

/**
 * @author wspride
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestContext.class)
public class CaseDbIndexTests extends BaseTestClass {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        configureRestoreFactory("synctestdomain", "synctestusername");
    }

    @Override
    protected String getMockRestoreFileName() {
        return "restores/dbtests/case_create_index.xml";
    }

    /**
     * Tests for basic common case database queries
     */
    @Test
    public void testCaseCreateIndex() throws Exception {
        syncDb();
        EvaluationContext ec = TestStorageUtils.getEvaluationContextWithoutSession();
        evaluate("instance('casedb')/casedb/case[@case_id = 'test_case_id_child']/index/parent", "test_case_id", ec);
        evaluate("instance('casedb')/casedb/case[@case_id = 'test_case_id']/index/missing", "", ec);
        evaluate("instance('casedb')/casedb/case[@case_type = 'unit_test_child'][index/parent = 'test_case_id_2']/@case_id",
                "test_case_id_child_2", ec);
    }

    public static void evaluate(String xpath, String expectedValue, EvaluationContext ec) {
        XPathExpression expr;
        try {
            expr = XPathParseTool.parseXPath(xpath);
            String result = FunctionUtils.toString(expr.eval(ec));
            assertEquals("XPath: " + xpath, expectedValue, result);
        } catch (XPathSyntaxException e) {
            e.printStackTrace();
        }
    }


}
