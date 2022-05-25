package org.commcare.formplayer.web.client;

import com.google.common.collect.Multimap;

import org.commcare.formplayer.services.RestoreFactory;
import org.commcare.formplayer.util.RequestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.net.URI;

@Component
public class WebClient {

    RestTemplate restTemplate;

    RestoreFactory restoreFactory;

    public String get(String url) {
        URI uri = URI.create(url);
        return get(uri, String.class);
    }

    public String get(URI uri) {
        return get(uri, String.class);
    }

    public <T> T get(URI uri, Class<T> responseType) {
        return getRaw(uri, responseType).getBody();
    }

    public <T> ResponseEntity<T> getRaw(URI uri, Class<T> responseType) {
        return restTemplate.exchange(
                RequestEntity.get(uri).headers(restoreFactory.getRequestHeaders(uri)).build(),
                responseType
        );
    }

    public <T> String post(String url, T body) {
        checkHmac();
        URI uri = URI.create(url);
        return restTemplate.exchange(
                RequestEntity.post(uri).headers(restoreFactory.getRequestHeaders(uri)).body(body),
                String.class
        ).getBody();
    }

    public <T> String postFormData(String url, Multimap<String, String> data) {
        checkHmac();
        URI uri = URI.create(url);
        LinkedMultiValueMap<String, String> postData = new LinkedMultiValueMap<>();
        data.forEach(postData::add);
        return restTemplate.exchange(
                RequestEntity.post(uri)
                        .headers(restoreFactory.getRequestHeaders(uri))
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                        .body(postData),
                String.class
        ).getBody();
    }

    public <T> Boolean caseClaimPost(String url, T body) {
        checkHmac();
        URI uri = URI.create(url);
        ResponseEntity<String> entity = restTemplate.exchange(
                RequestEntity.post(uri).headers(restoreFactory.getRequestHeaders(uri)).body(body),
                String.class
        );
        Boolean shouldSync = true;
        if (entity != null && entity.getStatusCode() == HttpStatus.NO_CONTENT) {
            shouldSync = false;
        }
        return shouldSync;
    }

    /**
     * This is not a technical limitation, just a code limitation that should be fixed in the
     * future.
     */
    private void checkHmac() {
        if (RequestUtils.requestAuthedWithHmac()) {
            throw new RuntimeException("HMAC auth not supported for POST requests");
        }
    }

    @Autowired
    public void setRestoreFactory(RestoreFactory restoreFactory) {
        this.restoreFactory = restoreFactory;
    }

    @Autowired
    public void setRestTemplate(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }
}
