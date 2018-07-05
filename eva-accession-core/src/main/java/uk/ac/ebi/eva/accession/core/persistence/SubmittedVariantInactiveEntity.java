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

import org.springframework.data.mongodb.core.mapping.Document;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.InactiveSubDocument;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;

import java.util.Objects;

@Document
public class SubmittedVariantInactiveEntity extends InactiveSubDocument<Long> implements ISubmittedVariant {

    private String assemblyAccession;

    private int taxonomyAccession;

    private String projectAccession;

    private String contig;

    private long start;

    private String referenceAllele;

    private String alternateAllele;

    private Long clusteredVariantAccession;

    private boolean supportedByEvidence;

    private Boolean matchesAssembly;

    SubmittedVariantInactiveEntity() {
    }

    public SubmittedVariantInactiveEntity(SubmittedVariantEntity submittedVariantEntity) {
        super(submittedVariantEntity);
        this.assemblyAccession = submittedVariantEntity.getAssemblyAccession();
        this.taxonomyAccession = submittedVariantEntity.getTaxonomyAccession();
        this.projectAccession = submittedVariantEntity.getProjectAccession();
        this.contig = submittedVariantEntity.getContig();
        this.start = submittedVariantEntity.getStart();
        this.referenceAllele = submittedVariantEntity.getReferenceAllele();
        this.alternateAllele = submittedVariantEntity.getAlternateAllele();
        this.clusteredVariantAccession = submittedVariantEntity.getClusteredVariantAccession();
        this.supportedByEvidence = submittedVariantEntity.isSupportedByEvidence();
        this.matchesAssembly = submittedVariantEntity.getMatchesAssembly();
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
    public Long getClusteredVariantAccession() {
        return clusteredVariantAccession;
    }

    @Override
    public boolean isSupportedByEvidence() {
        return supportedByEvidence;
    }

    @Override
    public Boolean getMatchesAssembly() {
        return matchesAssembly;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof ISubmittedVariant)) return false;

        ISubmittedVariant that = (ISubmittedVariant) o;

        if (taxonomyAccession != that.getTaxonomyAccession()) return false;
        if (start != that.getStart()) return false;
        if (!assemblyAccession.equals(that.getAssemblyAccession())) return false;
        if (!projectAccession.equals(that.getProjectAccession())) return false;
        if (!contig.equals(that.getContig())) return false;
        if (!referenceAllele.equals(that.getReferenceAllele())) return false;
        return alternateAllele.equals(that.getAlternateAllele());
    }

    @Override
    public int hashCode() {
        return Objects.hash(assemblyAccession, taxonomyAccession, projectAccession, contig, start,
                            referenceAllele, alternateAllele);
    }
}
