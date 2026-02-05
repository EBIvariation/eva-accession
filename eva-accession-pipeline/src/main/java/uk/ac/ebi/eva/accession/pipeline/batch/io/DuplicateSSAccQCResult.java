package uk.ac.ebi.eva.accession.pipeline.batch.io;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.List;

public class DuplicateSSAccQCResult {
    private Long sveAccession;
    private List<SubmittedVariantEntity> submittedVariantEntityList;

    public DuplicateSSAccQCResult(Long sveAccession, List<SubmittedVariantEntity> submittedVariantEntityList) {
        this.sveAccession = sveAccession;
        this.submittedVariantEntityList = submittedVariantEntityList;
    }

    public Long getSveAccession() {
        return sveAccession;
    }

    public void setSveAccession(Long sveAccession) {
        this.sveAccession = sveAccession;
    }

    public List<SubmittedVariantEntity> getSubmittedVariantEntityList() {
        return submittedVariantEntityList;
    }

    public void setSubmittedVariantEntityList(List<SubmittedVariantEntity> submittedVariantEntityList) {
        this.submittedVariantEntityList = submittedVariantEntityList;
    }
}
