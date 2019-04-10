/*
 *
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
 *
 */

package uk.ac.ebi.eva.accession.ws.dto;

import java.util.List;

public class BeaconAlleleRequest {

    private String alternateBases;

    private String referenceBases;

    private String referenceName;

    private long start;

    private String assemblyId;

    private List<String> datasetIds;

    private boolean includeDatasetResponses;

    public BeaconAlleleRequest(String alternateBases, String referenceBases, String referenceName, long start,
                               String assemblyId, List<String> datasetIds, boolean includeDatasetResponses) {
        this.alternateBases = alternateBases;
        this.referenceBases = referenceBases;
        this.referenceName = referenceName;
        this.start = start;
        this.assemblyId = assemblyId;
        this.datasetIds = datasetIds;
        this.includeDatasetResponses = includeDatasetResponses;
    }

    public String getAlternateBases() {
        return alternateBases;
    }

    public String getReferenceBases() {
        return referenceBases;
    }

    public String getReferenceName() {
        return referenceName;
    }

    public long getStart() {
        return start;
    }

    public String getAssemblyId() {
        return assemblyId;
    }

    public List<String> getDatasetIds() {
        return datasetIds;
    }

    public boolean isIncludeDatasetResponses() {
        return includeDatasetResponses;
    }
}
