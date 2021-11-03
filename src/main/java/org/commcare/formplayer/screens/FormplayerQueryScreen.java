package org.commcare.formplayer.screens;

import org.commcare.session.RemoteQuerySessionManager;
import org.commcare.suite.model.QueryPrompt;
import org.commcare.util.screen.QueryScreen;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.google.common.collect.*;

/**
 * Created by willpride on 8/7/16.
 */
public class FormplayerQueryScreen extends QueryScreen {

    public FormplayerQueryScreen() {
        super(null, null, null);
    }

    public MultiValueMap getRequestData(boolean skipDefaultPromptValues) {
        MultiValueMap ret = new LinkedMultiValueMap<String, String>();
        Multimap<String, String> queryParams = getQueryParams(skipDefaultPromptValues);
        for (String key : queryParams.keySet()) {
            QueryPrompt prompt = userInputDisplays.get(key);
            for (String value : queryParams.get(key)) {
                if (prompt != null) {
                    String[] choices = RemoteQuerySessionManager.extractMultipleChoices(value);
                    for (String choice : choices) {
                        ret.add(key, choice);
                    }
                } else {
                    ret.add(key, value);
                }
            }
        }
        return ret;
    }

}
