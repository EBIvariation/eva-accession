package uk.ac.ebi.eva.accession.release.steps.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;

import static org.springframework.util.StringUtils.hasText;

public class ContigProcessor implements ItemProcessor<String, String> {

    private static final Logger logger = LoggerFactory.getLogger(ContigProcessor.class);

    private ContigMapping contigMapping;

    public ContigProcessor(ContigMapping contigMapping) {
        this.contigMapping = contigMapping;
    }

    @Override
    public String process(String contig) throws Exception {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contig);
        String genBankContig;
        if (contigSynonyms != null && hasText(genBankContig = contigSynonyms.getGenBank())) {
            return genBankContig;
        }
        logger.warn("INSDC synonym not found for contig {}", contig);
        return contig;
    }
}
