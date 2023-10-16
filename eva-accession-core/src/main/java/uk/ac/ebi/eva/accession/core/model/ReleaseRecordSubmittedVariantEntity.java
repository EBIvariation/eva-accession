/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.core.model;

import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Objects;

@Document
public class ReleaseRecordSubmittedVariantEntity {

    private final long accession;

    private final String hashedMessage;

    @Field("study")
    private final String projectAccession;

    private final String contig;

    private final long start;

    // Retain original ref/alt for traceability
    // Add separate fields for ref/alt with context bases
    @Field("ref")
    private final String referenceAllele;

    @Field("alt")
    private final String alternateAllele;

    @Field("refWithCtxBase")
    private final String referenceAlleleWithContextBase;

    @Field("altWithCtxBase")
    private final String alternateAlleleWithContextBase;

    @Field("evidence")
    private final Boolean supportedByEvidence;

    @Field("asmMatch")
    private final Boolean assemblyMatch;

    @Field("allelesMatch")
    private final Boolean allelesMatch;

    @Field("validated")
    private final Boolean validated;

    public ReleaseRecordSubmittedVariantEntity(long accession, String hashedMessage, String projectAccession,
                                               String contig, long start, String referenceAllele,
                                               String alternateAllele, String referenceAlleleWithContextBase,
                                               String alternateAlleleWithContextBase, Boolean supportedByEvidence,
                                               Boolean assemblyMatch, Boolean allelesMatch, Boolean validated) {
        this.accession = accession;
        this.hashedMessage = hashedMessage;
        this.projectAccession = projectAccession;
        this.contig = contig;
        this.start = start;
        this.referenceAllele = Objects.nonNull(referenceAllele) ? referenceAllele.toUpperCase() : null;
        this.alternateAllele = Objects.nonNull(alternateAllele) ? alternateAllele.toUpperCase() : null;
        this.referenceAlleleWithContextBase = Objects.nonNull(referenceAlleleWithContextBase) ?
                referenceAlleleWithContextBase.toUpperCase() : null;
        this.alternateAlleleWithContextBase = Objects.nonNull(alternateAlleleWithContextBase) ?
                alternateAlleleWithContextBase.toUpperCase() : null;
        this.supportedByEvidence = supportedByEvidence;
        this.assemblyMatch = assemblyMatch;
        this.allelesMatch = allelesMatch;
        this.validated = validated;
    }

    public long getAccession() {
        return accession;
    }

    public String getHashedMessage() {
        return hashedMessage;
    }

    public String getProjectAccession() {
        return projectAccession;
    }

    public String getContig() {
        return contig;
    }

    public long getStart() {
        return start;
    }

    public String getReferenceAllele() {
        return referenceAllele;
    }

    public String getAlternateAllele() {
        return alternateAllele;
    }

    public String getReferenceAlleleWithContextBase() {
        return referenceAlleleWithContextBase;
    }

    public String getAlternateAlleleWithContextBase() {
        return alternateAlleleWithContextBase;
    }

    public Boolean getSupportedByEvidence() {
        return supportedByEvidence;
    }

    public Boolean getAssemblyMatch() {
        return assemblyMatch;
    }

    public Boolean getAllelesMatch() {
        return allelesMatch;
    }

    public Boolean getValidated() {
        return validated;
    }
}
