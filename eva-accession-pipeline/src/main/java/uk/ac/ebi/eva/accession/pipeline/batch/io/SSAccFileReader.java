package uk.ac.ebi.eva.accession.pipeline.batch.io;

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
 * Read all SubmittedVariant Accessions from VCF file in batches
 */
public class SSAccFileReader implements ItemStreamReader<List<Long>> {
    private static final Logger logger = LoggerFactory.getLogger(SSAccFileReader.class);

    private BufferedReader reader;
    private String vcfFileWithSSAcc;
    private int chunkSize;

    public SSAccFileReader(String vcfFileWithSSAcc, int chunkSize) {
        this.vcfFileWithSSAcc = vcfFileWithSSAcc;
        this.chunkSize = chunkSize;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            reader = new BufferedReader(new FileReader(vcfFileWithSSAcc));
        } catch (IOException e) {
            throw new ItemStreamException("Failed to open the file (" + vcfFileWithSSAcc + ") with submitted variant accessions", e);
        }
    }


    @Override
    public List<Long> read() {
        List<Long> submittedVariantIds = new ArrayList<>();
        String line;

        try {
            while (submittedVariantIds.size() < chunkSize && (line = reader.readLine()) != null) {
                if (!line.isEmpty() && !line.startsWith("#")) {
                    String ssAcc = line.split("[ \t]+")[2].trim().substring(2);
                    submittedVariantIds.add(Long.parseLong(ssAcc));
                }
            }
            if (submittedVariantIds.isEmpty()) {
                return null;
            } else {
                return submittedVariantIds;
            }
        } catch (IOException e) {
            throw new ItemStreamException("Error reading submitted variant Accessions from file", e);
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
            throw new ItemStreamException("Failed to close file: " + vcfFileWithSSAcc, e);
        }
    }
}

