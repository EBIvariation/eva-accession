package uk.ac.ebi.eva.accession.clustering.batch.io.qc;

import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DuplicateRSAccQCResult {
    private Long cveAccession;
    private List<ClusteredVariantEntity> clusteredVariantEntityList;
    private Map<String, Set<SubmittedVariantEntity>> submittedVariantEntityMap;

    public DuplicateRSAccQCResult(Long cveAccession, List<ClusteredVariantEntity> clusteredVariantEntityList,
                                  Map<String, Set<SubmittedVariantEntity>> submittedVariantEntityMap) {
        this.cveAccession = cveAccession;
        this.clusteredVariantEntityList = clusteredVariantEntityList;
        this.submittedVariantEntityMap = submittedVariantEntityMap;
    }

    public Long getCveAccession() {
        return cveAccession;
    }

    public void setCveAccession(Long cveAccession) {
        this.cveAccession = cveAccession;
    }

    public List<ClusteredVariantEntity> getClusteredVariantEntityList() {
        return clusteredVariantEntityList;
    }

    public void setClusteredVariantEntityList(List<ClusteredVariantEntity> clusteredVariantEntityList) {
        this.clusteredVariantEntityList = clusteredVariantEntityList;
    }

    public Map<String, Set<SubmittedVariantEntity>> getSubmittedVariantEntityMap() {
        return submittedVariantEntityMap;
    }

    public void setSubmittedVariantEntityMap(Map<String, Set<SubmittedVariantEntity>> submittedVariantEntityMap) {
        this.submittedVariantEntityMap = submittedVariantEntityMap;
    }
}
