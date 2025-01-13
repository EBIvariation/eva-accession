package uk.ac.ebi.eva.accession.clustering.batch.io.qc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class DuplicateRSAccQCWriter implements ItemStreamWriter<List<Long>> {
    private static final Logger logger = LoggerFactory.getLogger(DuplicateRSAccQCWriter.class);
    private String duplicateRSAccFile;
    private BufferedWriter writer;

    public DuplicateRSAccQCWriter(String duplicateRSAccFile) {
        this.duplicateRSAccFile = duplicateRSAccFile;
    }

    @Override
    public void open(org.springframework.batch.item.ExecutionContext executionContext) throws ItemStreamException {
        try {
            writer = new BufferedWriter(new FileWriter(duplicateRSAccFile, true));
        } catch (IOException e) {
            throw new ItemStreamException("Failed to open the file (" + duplicateRSAccFile + ") to write duplicate RS accessions", e);
        }
    }

    @Override
    public void update(org.springframework.batch.item.ExecutionContext executionContext) throws ItemStreamException {
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            throw new ItemStreamException("Failed to close file: " + duplicateRSAccFile, e);
        }
    }

    @Override
    public void write(List<? extends List<Long>> items) throws Exception {
        for (List<Long> list : items) {
            if (list != null && !list.isEmpty()) {
                appendToFile(list);
            } else {
                logger.info("No duplicate RS IDs in the batch to append");
            }
        }
    }

    private void appendToFile(List<Long> duplicateRSIdList) throws IOException {
        for (Long rsId : duplicateRSIdList) {
            writer.write(rsId.toString());
            writer.newLine();
        }
        logger.warn("Appended " + duplicateRSIdList.size() + " duplicate RS IDs to the file");
    }
}