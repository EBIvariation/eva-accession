package uk.ac.ebi.eva.accession.dbsnp.contig;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContigMapWrapper {

    private Map<String, ContigSynonyms> SequenceNameMap;

    private Map<String, ContigSynonyms> GenBankMap;

    private Map<String, ContigSynonyms> RefSeqMap;

    private Map<String, ContigSynonyms> UcscMap;

    private static final String CHROMOSOME_PATTERN = "(chromosome|chrom|chr|ch)(.+)";

    private static final Pattern PATTERN = Pattern.compile(CHROMOSOME_PATTERN, Pattern.CASE_INSENSITIVE);

    public ContigMapWrapper() {
        SequenceNameMap = new HashMap<>();
        GenBankMap = new HashMap<>();
        RefSeqMap = new HashMap<>();
        UcscMap = new HashMap<>();
    }

    public ContigSynonyms getSynonyms(String contig) {
        contig = removePrefix(contig);
        ContigSynonyms contigSynonyms;
        if ((contigSynonyms = SequenceNameMap.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = GenBankMap.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = RefSeqMap.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = UcscMap.get(contig)) != null) {
            return contigSynonyms;
        }
        return null;
    }

    private String removePrefix(String contig) {
        Matcher matcher = PATTERN.matcher(contig);
        String contigNoPrefix = contig;
        if (matcher.matches()) {
            contigNoPrefix = matcher.group(2);
        }
        return contigNoPrefix;
    }

    public void fillContigConventionMaps(ContigSynonyms contigSynonyms) {
        contigSynonyms.setSequenceName(removePrefix(contigSynonyms.getSequenceName()));
        contigSynonyms.setUcsc(removePrefix(contigSynonyms.getUcsc()));
        SequenceNameMap.put(contigSynonyms.getSequenceName(), contigSynonyms);
        GenBankMap.put(contigSynonyms.getGenBank(), contigSynonyms);
        RefSeqMap.put(contigSynonyms.getRefSeq(), contigSynonyms);
        UcscMap.put(contigSynonyms.getUcsc(), contigSynonyms);
    }
}
