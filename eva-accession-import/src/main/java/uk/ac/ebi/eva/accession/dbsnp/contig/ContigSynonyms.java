package uk.ac.ebi.eva.accession.dbsnp.contig;

public class ContigSynonyms {

    private String sequenceName;

    private String genBank;

    private String refSeq;

    private String ucsc;

    public ContigSynonyms(String sequenceName, String genBank, String refSeq, String ucsc) {
        this.sequenceName = sequenceName;
        this.genBank = genBank;
        this.refSeq = refSeq;
        this.ucsc = ucsc;
    }

    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public String getGenBank() {
        return genBank;
    }

    public void setGenBank(String genBank) {
        this.genBank = genBank;
    }

    public String getRefSeq() {
        return refSeq;
    }

    public void setRefSeq(String refSeq) {
        this.refSeq = refSeq;
    }

    public String getUcsc() {
        return ucsc;
    }

    public void setUcsc(String ucsc) {
        this.ucsc = ucsc;
    }
}