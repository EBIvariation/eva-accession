/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.ws.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.rest.BasicRestControllerAdvice;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.ErrorMessage;

import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;

/**
 * This class activates the exception handling in {@link BasicRestControllerAdvice} for our controllers:
 * {@link ClusteredVariantsRestController} and {@link SubmittedVariantsRestController}.
 *
 * It overrides handleMergeExceptions to build the redirect URL from a configured base URL
 * rather than from the incoming Host header, preventing open-redirect via Host-header injection.
 */
@RestControllerAdvice(assignableTypes = {ClusteredVariantsRestController.class, SubmittedVariantsRestController.class})
public class EvaControllerAdvice extends BasicRestControllerAdvice {

    private static final Logger logger = LoggerFactory.getLogger(EvaControllerAdvice.class);

    @Value("${eva.api.base-url}")
    private String apiBaseUrl;

    @Override
    @ExceptionHandler(value = AccessionMergedException.class)
    public ResponseEntity<ErrorMessage> handleMergeExceptions(AccessionMergedException ex) {
        logger.error(ex.getMessage(), ex);
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.currentRequestAttributes()).getRequest();
        if (HttpMethod.GET.name().equals(request.getMethod())) {
            // Use getRequestURI() (path only, no host) and prepend the configured base URL
            // so that the redirect target cannot be influenced by the incoming Host header.
            String path = request.getRequestURI();
            int lastOccurrenceStart = path.lastIndexOf(ex.getOriginAccessionId());
            if (lastOccurrenceStart >= 0) {
                int lastOccurrenceEnd = lastOccurrenceStart + ex.getOriginAccessionId().length();
                String newPath = path.substring(0, lastOccurrenceStart)
                        + ex.getDestinationAccessionId()
                        + path.substring(lastOccurrenceEnd);
                return ResponseEntity.status(HttpStatus.MOVED_PERMANENTLY)
                                     .location(URI.create(apiBaseUrl + newPath))
                                     .body(new ErrorMessage(HttpStatus.MOVED_PERMANENTLY, ex, ex.getMessage()));
            }
        }
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                             .body(new ErrorMessage(HttpStatus.NOT_FOUND, ex, ex.getMessage()));
    }
}
