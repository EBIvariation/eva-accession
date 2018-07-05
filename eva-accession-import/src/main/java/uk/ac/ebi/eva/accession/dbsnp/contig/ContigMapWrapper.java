package uk.ac.ebi.eva.accession.dbsnp.contig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContigMapWrapper {

    private Map<String, ContigSynonym> SequenceNameMap;

    private Map<String, ContigSynonym> GenBankMap;

    private Map<String, ContigSynonym> RefSeqMap;

    private Map<String, ContigSynonym> UcscMap;

    public ContigMapWrapper() {
        SequenceNameMap = new HashMap<>();
        GenBankMap = new HashMap<>();
        RefSeqMap = new HashMap<>();
        UcscMap = new HashMap<>();
    }

    public ContigMapWrapper(
            Map<String, ContigSynonym> sequenceNameMap,
            Map<String, ContigSynonym> genBankMap,
            Map<String, ContigSynonym> refSeqMap,
            Map<String, ContigSynonym> ucscMap) {
        SequenceNameMap = sequenceNameMap;
        GenBankMap = genBankMap;
        RefSeqMap = refSeqMap;
        UcscMap = ucscMap;
    }

    public Map<String, ContigSynonym> getSequenceNameMap() {
        return SequenceNameMap;
    }

    public void setSequenceNameMap(
            Map<String, ContigSynonym> sequenceNameMap) {
        SequenceNameMap = sequenceNameMap;
    }

    public Map<String, ContigSynonym> getGenBankMap() {
        return GenBankMap;
    }

    public void setGenBankMap(Map<String, ContigSynonym> genBankMap) {
        GenBankMap = genBankMap;
    }

    public Map<String, ContigSynonym> getRefSeqMap() {
        return RefSeqMap;
    }

    public void setRefSeqMap(Map<String, ContigSynonym> refSeqMap) {
        RefSeqMap = refSeqMap;
    }

    public Map<String, ContigSynonym> getUcscMap() {
        return UcscMap;
    }

    public void setUcscMap(Map<String, ContigSynonym> ucscMap) {
        UcscMap = ucscMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContigMapWrapper that = (ContigMapWrapper) o;
        return Objects.equals(SequenceNameMap, that.SequenceNameMap) &&
                Objects.equals(GenBankMap, that.GenBankMap) &&
                Objects.equals(RefSeqMap, that.RefSeqMap) &&
                Objects.equals(UcscMap, that.UcscMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(SequenceNameMap, GenBankMap, RefSeqMap, UcscMap);
    }

    public String getSynonymByContigConvention(String contig, ContigNameConvention contigNameConvention) {
        ContigSynonym contigSynonym;
        if ((contigSynonym = getSynonyms(contig)) != null) {
            return getContig(contigSynonym, contigNameConvention);
        }
        return contig;
    }

    private ContigSynonym getSynonyms(String contig) {
        contig = removePrefix(contig);
        ContigSynonym contigSynonym;
        if ((contigSynonym = SequenceNameMap.get(contig)) != null) {
            return contigSynonym;
        }
        if ((contigSynonym = GenBankMap.get(contig)) != null) {
            return contigSynonym;
        }
        if ((contigSynonym = RefSeqMap.get(contig)) != null) {
            return contigSynonym;
        }
        if ((contigSynonym = UcscMap.get(contig)) != null) {
            return contigSynonym;
        }
        return null;
    }

    private String getContig(ContigSynonym contigSynonym, ContigNameConvention contigNameConvention) {
        switch (contigNameConvention) {
            case SEQUENCE_NAME:
                return contigSynonym.getSequenceName();
            case GEN_BANK:
                return contigSynonym.getGenBank();
            case REF_SEQ:
                return contigSynonym.getRefSeq();
            case UCSC:
                return contigSynonym.getUcsc();
            default:
                return null;
        }
    }

    private String removePrefix(String contig) {
        String chromosomePattern = "(chromosome|chrom|chr|ch)(.+)";
        Pattern pattern = Pattern.compile(chromosomePattern, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(contig);
        String contigNoPrefix = contig;
        if (matcher.matches()) {
            contigNoPrefix = matcher.group(2);
        }
        return contigNoPrefix;
    }

    public void fillContigConventionMaps(ContigSynonym contigSynonym) {
        contigSynonym.setSequenceName(removePrefix(contigSynonym.getSequenceName()));
        contigSynonym.setUcsc(removePrefix(contigSynonym.getUcsc()));
        SequenceNameMap.put(removePrefix(contigSynonym.getSequenceName()), contigSynonym);
        GenBankMap.put(contigSynonym.getGenBank(), contigSynonym);
        RefSeqMap.put(contigSynonym.getRefSeq(), contigSynonym);
        UcscMap.put(contigSynonym.getUcsc(), contigSynonym);
    }
}
