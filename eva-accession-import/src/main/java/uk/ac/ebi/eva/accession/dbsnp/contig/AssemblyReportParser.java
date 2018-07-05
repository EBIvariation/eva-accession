package uk.ac.ebi.eva.accession.dbsnp.contig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.core.io.UrlResource;

import java.net.MalformedURLException;

public class AssemblyReportParser {

    private static final Logger logger = LoggerFactory.getLogger(AssemblyReportParser.class);

    private static final int SEQNAME_COLUMN = 0;

    private static final int GENBANK_COLUMN = 4;

    private static final int RELATIONSHIP_COLUMN = 5;

    private static final int REFSEQ_COLUMN = 6;

    private static final int UCSC_COLUMN = 9;

    private static final String IDENTICAL_SEQUENCE = "=";

    private FlatFileItemReader<String> reader;

    private ContigMapWrapper contigMapWrapper;

    public AssemblyReportParser(String url) {
        this.contigMapWrapper = null;
        initializeReader(url);
    }

    private void initializeReader(String url) {
        reader = new FlatFileItemReader<>();
        try {
            reader.setResource(new UrlResource(url));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Contig mapping file location is invalid: " + url, e);
        }
        reader.setLineMapper(new PassThroughLineMapper());
        reader.open(new ExecutionContext());
    }

    public ContigMapWrapper fillContigMap() throws Exception {
        if (contigMapWrapper == null) {
            logger.debug("About to populate contig mapping");

            String line;
            contigMapWrapper = new ContigMapWrapper();
            while ((line = reader.read()) != null) {
                addContigSynonym(line, contigMapWrapper);
            }
            reader.close();

            logger.debug("Contig mapping populated");
        }
        return contigMapWrapper;
    }

    private void addContigSynonym(String line, ContigMapWrapper contigMapWrapper) {
        String[] columns = line.split("\t", -1);
        if (columns[RELATIONSHIP_COLUMN].equals(IDENTICAL_SEQUENCE)) {
            ContigSynonym contigSynonym = new ContigSynonym(columns[SEQNAME_COLUMN],
                                                            columns[GENBANK_COLUMN], columns[REFSEQ_COLUMN],
                                                            columns[UCSC_COLUMN]);
            contigMapWrapper.fillContigConventionMaps(contigSynonym);
        }
    }
}
