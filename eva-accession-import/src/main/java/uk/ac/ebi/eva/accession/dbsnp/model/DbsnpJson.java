package uk.ac.ebi.eva.accession.dbsnp.model;

import org.springframework.batch.core.configuration.annotation.StepScope;

/**
 * Dbsnp object model to store key fields from Dbsnp Json
 */
@StepScope
public class DbsnpJson {

    private int taxonomyAccession;

    private String projectAccession;

    private String contig;

    private long start;

    private String referenceAllele;

    private String alternateAllele;

    private Long clusteredVariantAccession;

    public DbsnpJson() {
        taxonomyAccession = 9606;
    }

    public int getTaxonomyAccession() {
        return taxonomyAccession;
    }

    public String getProjectAccession() {
        return projectAccession;
    }

    public String getContig() {
        return contig;
    }

    public long getStart() { return start;}

    public String getReferenceAllele() {
        return referenceAllele;
    }

    public String getAlternateAllele() {
        return alternateAllele;
    }

    public void setProjectAccession(String projectAccession) {
        this.projectAccession = projectAccession;
    }

    public void setContig(String contig) {
        this.contig = contig;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public void setReferenceAllele(String referenceAllele) {
        this.referenceAllele = referenceAllele;
    }

    public void setAlternateAllele(String alternateAllele) {
        this.alternateAllele = alternateAllele;
    }

    public void setClusteredVariantAccession(Long clusteredVariantAccession) {
        this.clusteredVariantAccession = clusteredVariantAccession;
    }
}
