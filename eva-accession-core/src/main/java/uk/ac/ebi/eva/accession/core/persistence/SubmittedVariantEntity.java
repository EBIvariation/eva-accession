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
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityListeners;
import javax.persistence.Id;
import java.time.LocalDateTime;

@Entity
@EntityListeners(AuditingEntityListener.class)
public class SubmittedVariantEntity implements ISubmittedVariant {

    @Id
    private Long accession;

    @Column(nullable = false, unique = true)
    private String hashedMessage;

    @Column(nullable = false)
    private String assemblyAccession;

    @Column(nullable = false)
    private int taxonomyAccession;

    @Column(nullable = false)
    private String projectAccession;

    @Column(nullable = false)
    private String contig;

    @Column(nullable = false)
    private long start;

    @Column(nullable = false)
    private String referenceAllele;

    @Column(nullable = false)
    private String alternateAllele;

    @Column(nullable = false)
    private boolean supportedByEvidence;

    @CreatedDate
    @Column(updatable = false)
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
}
