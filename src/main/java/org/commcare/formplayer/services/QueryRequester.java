package org.commcare.formplayer.services;

import lombok.extern.apachecommons.CommonsLog;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;


@Component
@CommonsLog
public class QueryRequester {

    @Autowired
    private RestTemplate restTemplate;

    public String makeQueryRequest(String uri, HttpHeaders headers) {
        ResponseEntity<String> response =
                null;
        try {
            response = restTemplate.exchange(
                    // Spring framework automatically encodes urls. This ensures we don't pass in an already
                    // encoded url.
                    URLDecoder.decode(uri, "UTF-8"),
                    HttpMethod.GET,
                    new HttpEntity<String>(headers),
                    String.class
            );
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        String responseBody = response.getBody();
        log.info(String.format("Query request to URL %s successful", uri));
        return responseBody;
    }
}
