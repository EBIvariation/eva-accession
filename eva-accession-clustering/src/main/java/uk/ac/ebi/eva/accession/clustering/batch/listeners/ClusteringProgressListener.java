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
import uk.ac.ebi.eva.accession.core.batch.listeners.GenericProgressListener;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClusteringProgressListener extends GenericProgressListener<Variant, SubmittedVariantEntity> {
    private static final String PROCESS = "clustering";
    private static final String SUBMITTED_VARIANTS = "submitted_variants";
    private static final String CREATED_VARIANTS = "created_variants";
    private static final String UPDATED_VARIANTS = "updated_variants";
    private static final String MERGED_VARIANTS = "merged_variants";
    private static final String NEW_CLUSTER_VARIANTS = "new_cluster_variants";

    private static final String URL = "http://localhost:8080/v1/countstats/bulk/count";

    private static final Logger logger = LoggerFactory.getLogger(ClusteringProgressListener.class);

    private final RestTemplate restTemplate;
    private final ClusteringCounts clusteringCounts;

    public ClusteringProgressListener(long chunkSize, ClusteringCounts clusteringCounts, RestTemplate restTemplate) {
        super(chunkSize);
        this.clusteringCounts = clusteringCounts;
        this.restTemplate = restTemplate;
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
            String assembly = stepExecution.getJobExecution().getJobParameters().getString("assemblyAccession");
            String identifier = createIdentifier(assembly);
            saveClusteringCountMetricsInDB(numTotalItemsRead, clusteringCounts, identifier);
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

    //TODO: numtotalItemsRead does not make sense here, check for others as well
    //TODO: how to get url here ("https://www.ebi.ac.uk/v1/countstats/bulk/count") <eva.context.root>https://www.ebi.ac.uk</eva.context.root>
    private void saveClusteringCountMetricsInDB(long numTotalItemsRead, ClusteringCounts clusteringCounts, String identifier) {
        List<Count> countList = new ArrayList<>();
        countList.add(new Count(PROCESS, identifier, SUBMITTED_VARIANTS, numTotalItemsRead));
        countList.add(new Count(PROCESS, identifier, CREATED_VARIANTS, clusteringCounts.getClusteredVariantsCreated()));
        countList.add(new Count(PROCESS, identifier, UPDATED_VARIANTS, clusteringCounts.getClusteredVariantsUpdated()));
        countList.add(new Count(PROCESS, identifier, MERGED_VARIANTS, clusteringCounts.getClusteredVariantsMergeOperationsWritten()));
        countList.add(new Count(PROCESS, identifier, NEW_CLUSTER_VARIANTS, clusteringCounts.getSubmittedVariantsClustered()));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

        HttpEntity<Object> requestEntity = new HttpEntity<>(countList, headers);
        ResponseEntity<List<Count>> response = restTemplate.exchange(URL, HttpMethod.POST, requestEntity,
                new ParameterizedTypeReference<List<Count>>() {
                });

        if (response.getStatusCode() == HttpStatus.OK) {
            logger.info("Metric Count successfully saved In DB");
        } else {
            logger.warn("Could not save count In DB");
        }
    }
}
