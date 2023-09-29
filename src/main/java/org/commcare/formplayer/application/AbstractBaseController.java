package org.commcare.formplayer.application;

import datadog.trace.api.Trace;
import lombok.extern.apachecommons.CommonsLog;
import org.commcare.formplayer.beans.InstallRequestBean;
import org.commcare.formplayer.beans.NotificationMessage;
import org.commcare.formplayer.beans.SessionNavigationBean;
import org.commcare.formplayer.engine.FormplayerConfigEngine;
import org.commcare.formplayer.objects.SerializableFormSession;
import org.commcare.formplayer.objects.SerializableMenuSession;
import org.commcare.formplayer.services.*;
import org.commcare.formplayer.session.FormSession;
import org.commcare.formplayer.session.MenuSession;
import org.commcare.formplayer.util.NotificationLogger;
import org.commcare.formplayer.util.serializer.SessionSerializer;
import org.commcare.session.CommCareSession;
import org.javarosa.core.model.actions.FormSendCalloutHandler;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;

import javax.servlet.http.HttpServletRequest;

/**
 * Base Controller class containing autowired beans used in both MenuController and FormController
 */
@CommonsLog
public abstract class AbstractBaseController {

    @Autowired
    protected FormSessionService formSessionService;

    @Autowired
    private FormDefinitionService formDefinitionService;

    @Autowired
    protected MenuSessionService menuSessionService;

    @Autowired
    protected InstallService installService;

    @Autowired
    protected RestoreFactory restoreFactory;

    @Autowired
    protected NewFormResponseFactory newFormResponseFactory;

    @Autowired
    protected FormplayerStorageFactory storageFactory;

    @Autowired
    protected FormSendCalloutHandler formSendCalloutHandler;

    @Autowired
    protected MenuSessionFactory menuSessionFactory;

    @Autowired
    protected MenuSessionRunnerService runnerService;

    @Autowired
    private VirtualDataInstanceService virtualDataInstanceService;


}
