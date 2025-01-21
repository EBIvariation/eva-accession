package uk.ac.ebi.eva.accession.clustering.batch.io.qc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Read all ClusteredVariant Accessions from file in batches
 */
public class DuplicateRSAccQCFileReader implements ItemStreamReader<List<Long>> {
    private static final Logger logger = LoggerFactory.getLogger(DuplicateRSAccQCFileReader.class);

    private BufferedReader reader;
    private String rsAccFile;
    private int chunkSize;

    public DuplicateRSAccQCFileReader(String rsAccFile, int chunkSize) {
        this.rsAccFile = rsAccFile;
        this.chunkSize = chunkSize;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            reader = new BufferedReader(new FileReader(rsAccFile));
        } catch (IOException e) {
            throw new ItemStreamException("Failed to open the file (" + rsAccFile + ") with clustered variant accessions", e);
        }
    }


    @Override
    public List<Long> read() {
        List<Long> clusteredVariantIds = new ArrayList<>();
        String line;

        try {
            while (clusteredVariantIds.size() < chunkSize && (line = reader.readLine()) != null) {
                String rsAcc = line.split("[ \t]+")[0].trim();
                clusteredVariantIds.add(Long.parseLong(rsAcc));
            }
            if (clusteredVariantIds.isEmpty()) {
                return null;
            } else {
                return clusteredVariantIds;
            }
        } catch (IOException e) {
            throw new ItemStreamException("Error reading variant Accessions from file", e);
        }
    }


    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            throw new ItemStreamException("Failed to close file: " + rsAccFile, e);
        }
    }
}

