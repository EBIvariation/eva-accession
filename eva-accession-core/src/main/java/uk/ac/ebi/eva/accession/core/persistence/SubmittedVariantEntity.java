/*
 *
 * Copyright 2018 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.core.persistence;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;

import java.time.LocalDateTime;

@Document
public class SubmittedVariantEntity implements ISubmittedVariant, Persistable<Long> {

    @Id
    private Long accession;

    @Indexed
    private String hashedMessage;

    private String assemblyAccession;

    private int taxonomyAccession;

    private String projectAccession;

    private String contig;

    private long start;

    private String referenceAllele;

    private String alternateAllele;

    private boolean supportedByEvidence;

    @CreatedDate
    private LocalDateTime createdDate;

    SubmittedVariantEntity() {
    }

    public SubmittedVariantEntity(Long accession, String hashedMessage, ISubmittedVariant model) {
        this(accession, hashedMessage, model.getAssemblyAccession(), model.getTaxonomyAccession(),
             model.getProjectAccession(), model.getContig(), model.getStart(), model.getReferenceAllele(),
             model.getAlternateAllele(), model.isSupportedByEvidence());
    }

    public SubmittedVariantEntity(Long accession, String hashedMessage, String assemblyAccession,
                                  int taxonomyAccession, String projectAccession, String contig, long start,
                                  String referenceAllele, String alternateAllele, boolean isSupportedByEvidence) {
        this.accession = accession;
        this.hashedMessage = hashedMessage;
        this.assemblyAccession = assemblyAccession;
        this.taxonomyAccession = taxonomyAccession;
        this.projectAccession = projectAccession;
        this.contig = contig;
        this.start = start;
        this.referenceAllele = referenceAllele;
        this.alternateAllele = alternateAllele;
        this.supportedByEvidence = isSupportedByEvidence;
    }

    public Long getAccession() {
        return this.accession;
    }

    public String getHashedMessage() {
        return hashedMessage;
    }

    @Override
    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    @Override
    public int getTaxonomyAccession() {
        return taxonomyAccession;
    }

    @Override
    public String getProjectAccession() {
        return projectAccession;
    }

    @Override
    public String getContig() {
        return contig;
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public String getReferenceAllele() {
        return referenceAllele;
    }

    @Override
    public String getAlternateAllele() {
        return alternateAllele;
    }

    @Override
    public boolean isSupportedByEvidence() {
        return supportedByEvidence;
    }

    @Override
    public LocalDateTime getCreatedDate() {
        return createdDate;
    }

    @Override
    public Long getId() {
        return accession;
    }

    /**
     * If we want to insert entities with the @Id already filled, this is needed for using @CreatedDate.
     *
     * Even if we implement the "insert" method in the repository to avoid updates and make inserts always, in
     * MongoTemplate::doInsertBatch, a BeforeConvertEvent is issued, and the listener processing that event, in
     * org.springframework.data.auditing.IsNewAwareAuditingHandler::markAudited it will check again if the entity is
     * new or not.
     *
     * An alternative explained here https://jira.spring.io/browse/DATAMONGO-946 is using @Version instead of
     * Persistable.
     */
    @Override
    public boolean isNew() {
        return true;
    }
}
