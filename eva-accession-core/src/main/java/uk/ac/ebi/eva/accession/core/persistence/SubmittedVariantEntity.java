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

import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.utils.ISubmittedVariantComparator;

import java.util.Objects;

@Document
public class SubmittedVariantEntity extends AccessionedDocument<Long> implements ISubmittedVariant {

    public static final boolean DEFAULT_SUPPORTED_BY_EVIDENCE = true;

    public static final boolean DEFAULT_ASSEMBLY_MATCH = true;

    public static final boolean DEFAULT_ALLELES_MATCH = true;

    public static final boolean DEFAULT_VALIDATED = false;

    @Field("asm")
    private String assemblyAccession;

    @Field("tax")
    private int taxonomyAccession;

    @Field("study")
    private String projectAccession;

    private String contig;

    private long start;

    @Field("ref")
    private String referenceAllele;

    @Field("alt")
    private String alternateAllele;

    @Indexed
    @Field("rs")
    private Long clusteredVariantAccession;

    @Field("evidence")
    private Boolean supportedByEvidence;

    @Field("asmMatch")
    private Boolean assemblyMatch;

    @Field("allelesMatch")
    private Boolean allelesMatch;

    @Field("validated")
    private Boolean validated;

    protected SubmittedVariantEntity() {
    }

    public SubmittedVariantEntity(Long accession, String hashedMessage, ISubmittedVariant model) {
        this(accession, hashedMessage, model.getAssemblyAccession(), model.getTaxonomyAccession(),
             model.getProjectAccession(), model.getContig(), model.getStart(), model.getReferenceAllele(),
             model.getAlternateAllele(), model.getClusteredVariantAccession(), model.isSupportedByEvidence(),
             model.isAssemblyMatch(), model.isAllelesMatch(), model.isValidated(), 1);
    }

    public SubmittedVariantEntity(Long accession, String hashedMessage, String assemblyAccession,
                                  int taxonomyAccession, String projectAccession, String contig, long start,
                                  String referenceAllele, String alternateAllele, Long clusteredVariantAccession,
                                  Boolean supportedByEvidence, Boolean assemblyMatch, Boolean allelesMatch,
                                  Boolean validated, int version) {
        super(hashedMessage, accession, version);
        this.assemblyAccession = assemblyAccession;
        this.taxonomyAccession = taxonomyAccession;
        this.projectAccession = projectAccession;
        this.contig = contig;
        this.start = start;
        this.referenceAllele = referenceAllele;
        this.alternateAllele = alternateAllele;
        this.clusteredVariantAccession = clusteredVariantAccession;
        this.supportedByEvidence =
                Objects.equals(supportedByEvidence, DEFAULT_SUPPORTED_BY_EVIDENCE) ? null : supportedByEvidence;
        this.assemblyMatch = Objects.equals(assemblyMatch, DEFAULT_ASSEMBLY_MATCH) ? null : assemblyMatch;
        this.allelesMatch = Objects.equals(allelesMatch, DEFAULT_ALLELES_MATCH) ? null : allelesMatch;
        this.validated = Objects.equals(validated, DEFAULT_VALIDATED) ? null : validated;
    }

    public ISubmittedVariant getModel() {
        this.supportedByEvidence = supportedByEvidence == null ? DEFAULT_SUPPORTED_BY_EVIDENCE : supportedByEvidence;
        this.assemblyMatch = assemblyMatch == null ? DEFAULT_ASSEMBLY_MATCH : assemblyMatch;
        this.allelesMatch = allelesMatch == null ? DEFAULT_ALLELES_MATCH : allelesMatch;
        this.validated = validated == null ? DEFAULT_VALIDATED : validated;
        return this;
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
    public Boolean isSupportedByEvidence() {
        return supportedByEvidence;
    }

    @Override
    public Boolean isAssemblyMatch() {
        return assemblyMatch;
    }

    @Override
    public Boolean isAllelesMatch() {
        return allelesMatch;
    }

    @Override
    public Boolean isValidated() {
        return validated;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ISubmittedVariant)) {
            return false;
        }

        return ISubmittedVariantComparator.equals(this, (ISubmittedVariant) o);
    }

    @Override
    public int hashCode() {
        return ISubmittedVariantComparator.hashCode(this);
    }

}
