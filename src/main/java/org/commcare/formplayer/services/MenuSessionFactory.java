package org.commcare.formplayer.services;

import com.google.common.collect.ImmutableMultimap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commcare.core.interfaces.RemoteInstanceFetcher;
import org.commcare.formplayer.engine.FormplayerConfigEngine;
import org.commcare.formplayer.objects.SerializableMenuSession;
import org.commcare.formplayer.session.MenuSession;
import org.commcare.session.CommCareSession;
import org.commcare.suite.model.MenuDisplayable;
import org.commcare.suite.model.RemoteQueryDatum;
import org.commcare.suite.model.SessionDatum;
import org.commcare.suite.model.StackFrameStep;
import org.commcare.util.screen.CommCareSessionException;
import org.commcare.util.screen.EntityScreen;
import org.commcare.util.screen.EntityScreenContext;
import org.commcare.util.screen.MenuScreen;
import org.commcare.util.screen.QueryScreen;
import org.commcare.util.screen.Screen;
import org.javarosa.core.model.instance.ExternalDataInstance;
import org.javarosa.xml.util.InvalidStructureException;
import org.javarosa.xml.util.UnfullfilledRequirementsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.xmlpull.v1.XmlPullParserException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Vector;

import datadog.trace.api.Trace;

/**
 * Class containing logic for accepting a NewSessionRequest and services,
 * restoring the user, opening the new form, and returning the question list response.
 */
@Component
public class MenuSessionFactory {

    private static final String NEXT_SCREEN = "NEXT_SCREEN";

    @Autowired
    private RestoreFactory restoreFactory;

    @Autowired
    private InstallService installService;

    @Autowired
    private CaseSearchHelper caseSearchHelper;

    @Autowired
    protected FormplayerStorageFactory storageFactory;

    @Autowired
    private VirtualDataInstanceService virtualDataInstanceService;

    @Value("${commcarehq.host}")
    private String host;

    private static final Log log = LogFactory.getLog(MenuSessionFactory.class);

    /**
     * Rebuild the MenuSession from its stack frame. This is used after end of form navigation.
     * By re-walking the frame, we establish the set of selections the user 'would' have made to get
     * to this state without doing end of form navigation. Such a path must always exist in a valid app.
     */
    @Trace
    public void rebuildSessionFromFrame(MenuSession menuSession, CaseSearchHelper caseSearchHelper) throws CommCareSessionException, RemoteInstanceFetcher.RemoteInstanceException {
        Vector<StackFrameStep> steps = menuSession.getSessionWrapper().getFrame().getSteps();
        menuSession.resetSession();
        EntityScreenContext entityScreenContext = new EntityScreenContext();
        Screen screen = menuSession.getNextScreen(false, entityScreenContext);
        int processedStepsCount = 0;
        boolean needsFullInit = false;
        while (screen != null) {
            String currentStep = null;
            if (screen instanceof MenuScreen) {
                if (menuSession.autoAdvanceMenu(screen, storageFactory.getPropertyManager().isAutoAdvanceMenu())) {
                    screen = menuSession.getNextScreen(needsFullInit, entityScreenContext);
                    continue;
                }

                MenuDisplayable[] options = ((MenuScreen)screen).getMenuDisplayables();
                for (int i = 0; i < options.length; i++) {
                    for (StackFrameStep step : steps) {
                        if (step.getId().equals(options[i].getCommandID())) {
                            currentStep = String.valueOf(i);
                            // final step, needs to init fully to show to screen
                            needsFullInit = ++processedStepsCount == steps.size();
                        }
                    }
                }
            } else if (screen instanceof EntityScreen) {
                EntityScreen entityScreen = (EntityScreen)screen;
                entityScreen.initReferences(menuSession.getSessionWrapper());
                SessionDatum neededDatum = entityScreen.getSession().getNeededDatum();
                for (StackFrameStep step : steps) {
                    if (step.getId().equals(neededDatum.getDataId())) {
                        if (entityScreen.referencesContainStep(step.getValue())) {
                            currentStep = step.getValue();
                            needsFullInit = ++processedStepsCount == steps.size();
                        }
                        break;
                    }
                }

                // Only init subscreen if we are not going to auto-launch a different screen
                String nextInput = currentStep == null ? "" : currentStep;
                entityScreen.evaluateAutoLaunch(nextInput);
                if (entityScreen.getAutoLaunchAction() == null) {
                    entityScreen.initListSubScreen();
                }

                if (currentStep != null && currentStep != NEXT_SCREEN && entityScreen.shouldBeSkipped()) {
                    menuSession.handleInput(currentStep, needsFullInit, true, false, entityScreenContext);
                    screen = menuSession.getNextScreen(needsFullInit, entityScreenContext);
                    continue;
                }
            } else if (screen instanceof QueryScreen) {
                QueryScreen queryScreen = (QueryScreen)screen;
                RemoteQueryDatum neededDatum = (RemoteQueryDatum) menuSession.getSessionWrapper().getNeededDatum();
                for (StackFrameStep step : steps) {
                    if (step.getId().equals(neededDatum.getDataId())) {
                        URI uri = null;
                        try {
                            uri = new URI(step.getValue());
                        } catch (URISyntaxException e) {
                            e.printStackTrace();
                            throw new CommCareSessionException("Query URL format error: " + e.getMessage(), e);
                        }
                        ImmutableMultimap.Builder<String, String> dataBuilder = ImmutableMultimap.builder();
                        step.getExtras().forEach((key, value) -> dataBuilder.putAll(key, ((Collection)value)));
                        try {
                            ExternalDataInstance searchDataInstance = caseSearchHelper.getRemoteDataInstance(
                                queryScreen.getQueryDatum().getDataId(),
                                queryScreen.getQueryDatum().useCaseTemplate(),
                                uri.toURL(),
                                dataBuilder.build(),
                                false
                            );
                            queryScreen.updateSession(searchDataInstance);
                            screen = menuSession.getNextScreen(needsFullInit, entityScreenContext);
                            currentStep = NEXT_SCREEN;
                            break;
                        } catch (InvalidStructureException | IOException | XmlPullParserException | UnfullfilledRequirementsException e) {
                            e.printStackTrace();
                            throw new CommCareSessionException("Query response format error: " + e.getMessage(), e);
                        }
                    }
                }
            }
            if (currentStep == null) {
                break;
            } else if (currentStep != NEXT_SCREEN) {
                menuSession.handleInput(currentStep, needsFullInit, true, false, entityScreenContext);
                menuSession.addSelection(currentStep);
                screen = menuSession.getNextScreen(needsFullInit, entityScreenContext);
            }
        }
    }

    @Trace
    public MenuSession buildSession(String username,
                                    String domain,
                                    String appId,
                                    String locale,
                                    boolean oneQuestionPerScreen,
                                    String asUser,
                                    boolean preview) throws Exception {
        return new MenuSession(username, domain, appId, locale,
                installService, restoreFactory, host, oneQuestionPerScreen, asUser, preview,
                new FormplayerRemoteInstanceFetcher(caseSearchHelper, virtualDataInstanceService));
    }

    @Trace
    public MenuSession buildSession(SerializableMenuSession serializableMenuSession, FormplayerConfigEngine engine,
            CommCareSession commCareSession) throws Exception {
        return new MenuSession(serializableMenuSession, engine, commCareSession, restoreFactory,
                new FormplayerRemoteInstanceFetcher(caseSearchHelper, virtualDataInstanceService));
    }
}
