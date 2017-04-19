package api.process;

import org.commcare.core.interfaces.UserSandbox;
import org.commcare.core.process.XmlFormRecordProcessor;
import org.commcare.data.xml.TransactionParserFactory;
import org.javarosa.xml.util.InvalidStructureException;
import org.javarosa.xml.util.UnfullfilledRequirementsException;
import org.xmlpull.v1.XmlPullParserException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 *  * Convenience methods, mostly for Touchforms so we don't have to deal with Java IO
 * in Jython which is terrible
 *
 * Created by wpride1 on 8/20/15.
 */
public class FormRecordProcessorHelper extends XmlFormRecordProcessor {

    public static void processXML(TransactionParserFactory factory, String fileText) throws IOException, XmlPullParserException, UnfullfilledRequirementsException, InvalidStructureException {
        InputStream stream = new ByteArrayInputStream(fileText.getBytes("UTF-8"));
        FormRecordProcessorThread thread = new FormRecordProcessorThread(factory, stream);
        thread.start();
    }


    public static void processXML(UserSandbox sandbox, String fileText) throws IOException, XmlPullParserException, UnfullfilledRequirementsException, InvalidStructureException {
        InputStream stream = new ByteArrayInputStream(fileText.getBytes("UTF-8"));
        FormRecordProcessorThread thread = new FormRecordProcessorThread(sandbox, stream);
        thread.start();
    }

    public static void processFile(UserSandbox sandbox, File record) throws IOException, XmlPullParserException, UnfullfilledRequirementsException, InvalidStructureException {
        InputStream stream = new FileInputStream(record);
        FormRecordProcessorThread thread = new FormRecordProcessorThread(sandbox, stream);
        thread.start();
    }

    static class FormRecordProcessorThread extends Thread {
        public FormRecordProcessorThread(UserSandbox sandbox, InputStream stream) throws IOException, XmlPullParserException, UnfullfilledRequirementsException, InvalidStructureException {
            process(sandbox, stream);
        }

        public FormRecordProcessorThread(TransactionParserFactory factory, InputStream stream) throws IOException, XmlPullParserException, UnfullfilledRequirementsException, InvalidStructureException {
            process(stream, factory);
        }
    }
}
