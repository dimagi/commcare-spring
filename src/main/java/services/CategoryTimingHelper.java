package services;

import com.timgroup.statsd.StatsDClient;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import util.Constants;
import util.FormplayerHttpRequest;
import util.FormplayerRaven;
import util.SimpleTimer;

@Component
public class CategoryTimingHelper {
    @Autowired
    private FormplayerHttpRequest request;

    @Autowired
    private StatsDClient datadogStatsDClient;

    @Autowired
    private FormplayerRaven raven;

    public class RecordingTimer extends SimpleTimer {
        private CategoryTimingHelper parent;
        private String category, sentryMessage;

        private RecordingTimer(CategoryTimingHelper parent, String category) {
            this.parent = parent;
            this.category = category;
        }

        @Override
        public RecordingTimer end() {
            super.end();
            return this;
        }

        public RecordingTimer setMessage(String message) {
            this.sentryMessage = message;
            return this;
        }

        public void record() {
            parent.recordCategoryTiming(this, category, sentryMessage);
        }
    }

    public RecordingTimer newTimer(String category) {
        return new RecordingTimer(this, category);
    }

    private void recordCategoryTiming(SimpleTimer timer, String category, String sentryMessage) {
        raven.newBreadcrumb()
                .setCategory(category)
                .setMessage(sentryMessage)
                .setData("duration", timer.formatDuration())
                .record();

        datadogStatsDClient.recordExecutionTime(
                Constants.DATADOG_GRANULAR_TIMINGS,
                timer.durationInMs(),
                "category:" + category,
                "request:" + getRequestEndpoint()
        );
    }

    private String getRequestEndpoint() {
        return StringUtils.strip(request.getRequestURI(), "/");
    }
}
