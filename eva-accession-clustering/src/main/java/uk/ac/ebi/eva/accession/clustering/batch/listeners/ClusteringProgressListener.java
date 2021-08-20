/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.batch.listeners;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.core.batch.listeners.GenericProgressListener;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClusteringProgressListener extends GenericProgressListener<Variant, SubmittedVariantEntity> {
    private static final String URL_PATH_SAVE_COUNT = "/v1/bulk/count";
    private static final String PROCESS = "clustering";

    private static final Logger logger = LoggerFactory.getLogger(ClusteringProgressListener.class);

    private final RestTemplate restTemplate;
    private final ClusteringCounts clusteringCounts;
    private final InputParameters inputParameters;
    private final String countStatsBaseURL;

    public ClusteringProgressListener(InputParameters inputParameters, ClusteringCounts clusteringCounts,
                                      RestTemplate restTemplate, String countStatsBaseURL) {
        super(inputParameters.getChunkSize());
        this.inputParameters = inputParameters;
        this.clusteringCounts = clusteringCounts;
        this.restTemplate = restTemplate;
        this.countStatsBaseURL = countStatsBaseURL;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        ExitStatus status = super.afterStep(stepExecution);

        String stepName = stepExecution.getStepName();
        long numTotalItemsRead = stepExecution.getReadCount();
        logger.info("Step {} finished: Items (ss) read = {}, rs created = {}, rs updated = {}, " +
                        "rs merge operations = {}, ss kept unclustered = {}, " +
                        "ss clustered = {}, ss updated rs merged = {}, ss update operations = {}",
                stepName, numTotalItemsRead,
                clusteringCounts.getClusteredVariantsCreated(),
                clusteringCounts.getClusteredVariantsUpdated(),
                clusteringCounts.getClusteredVariantsMergeOperationsWritten(),
                clusteringCounts.getSubmittedVariantsKeptUnclustered(),
                clusteringCounts.getSubmittedVariantsClustered(),
                clusteringCounts.getSubmittedVariantsUpdatedRs(),
                clusteringCounts.getSubmittedVariantsUpdateOperationWritten());
        try {
            String assembly = inputParameters.getAssemblyAccession();
            String identifier = createIdentifier(assembly);
            String url = countStatsBaseURL + URL_PATH_SAVE_COUNT;
            saveClusteringCountMetricsInDB(url, numTotalItemsRead, clusteringCounts, identifier);
        } catch (JSONException | RestClientException ex) {
            logger.error("Error occurred while saving Counts to DB", ex);
        }

        return status;
    }

    private String createIdentifier(String assembly) throws JSONException {
        JSONObject identifier = new JSONObject();
        identifier.put("assembly", assembly);
        return identifier.toString();
    }

    private void saveClusteringCountMetricsInDB(String url, long numTotalItemsRead, ClusteringCounts clusteringCounts, String identifier) {
        List<Count> countList = new ArrayList<>();
        countList.add(new Count(PROCESS, identifier, Metric.SUBMITTED_VARIANTS.getName(), numTotalItemsRead));
        countList.add(new Count(PROCESS, identifier, Metric.CREATED_VARIANTS.getName(), clusteringCounts.getClusteredVariantsCreated()));
        countList.add(new Count(PROCESS, identifier, Metric.UPDATED_VARIANTS.getName(), clusteringCounts.getClusteredVariantsUpdated()));
        countList.add(new Count(PROCESS, identifier, Metric.MERGED_VARIANTS.getName(), clusteringCounts.getClusteredVariantsMergeOperationsWritten()));
        countList.add(new Count(PROCESS, identifier, Metric.SUBMITTED_VARIANTS_CLUSTERED.getName(), clusteringCounts.getSubmittedVariantsClustered()));
        countList.add(new Count(PROCESS, identifier, Metric.SUBMITTED_VARIANTS_UNCLUSTERED.getName(), clusteringCounts.getSubmittedVariantsKeptUnclustered()));
        countList.add(new Count(PROCESS, identifier, Metric.SUBMITTED_VARIANTS_RS_MERGED.getName(), clusteringCounts.getSubmittedVariantsUpdatedRs()));
        countList.add(new Count(PROCESS, identifier, Metric.SUBMITTED_VARIANTS_UPDATED_OPERATIONS.getName(), clusteringCounts.getSubmittedVariantsUpdateOperationWritten()));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

        HttpEntity<Object> requestEntity = new HttpEntity<>(countList, headers);
        ResponseEntity<List<Count>> response = restTemplate.exchange(url, HttpMethod.POST, requestEntity,
                new ParameterizedTypeReference<List<Count>>() {
                });

        if (response.getStatusCode() == HttpStatus.OK) {
            logger.info("Metric Count successfully saved In DB");
        } else {
            throw new RestClientException("Could not save count In DB. HttpStatus code is " + response.getStatusCode());
        }
    }
}
