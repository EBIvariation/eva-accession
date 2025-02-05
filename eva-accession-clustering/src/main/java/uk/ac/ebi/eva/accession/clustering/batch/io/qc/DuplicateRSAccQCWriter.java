package uk.ac.ebi.eva.accession.clustering.batch.io.qc;

import gherkin.deps.com.google.gson.Gson;
import gherkin.deps.com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class DuplicateRSAccQCWriter implements ItemStreamWriter<List<DuplicateRSAccQCResult>> {
    private static final Logger logger = LoggerFactory.getLogger(DuplicateRSAccQCWriter.class);
    private String duplicateRSAccFile;
    private BufferedWriter writer;
    private final Gson gson = new GsonBuilder().create();

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
    public void write(List<? extends List<DuplicateRSAccQCResult>> listOfDuplicateRSAccQCResultLists) throws Exception {
        for (List<DuplicateRSAccQCResult> duplicateRSAccQCResultList : listOfDuplicateRSAccQCResultLists) {
            if (duplicateRSAccQCResultList != null && !duplicateRSAccQCResultList.isEmpty()) {
                appendToFile(duplicateRSAccQCResultList);
            } else {
                logger.info("No duplicate RS IDs in the batch to append");
            }
        }
    }

    private void appendToFile(List<DuplicateRSAccQCResult> duplicateRSAccQCResultsList) throws IOException {
        for (DuplicateRSAccQCResult duplicateRSAccQCResult : duplicateRSAccQCResultsList) {
            writer.write(duplicateRSAccQCResult.getCveAccession().toString() + " " + gson.toJson(duplicateRSAccQCResult));
            writer.newLine();
        }
        logger.warn("Appended {} duplicate RS IDs to the file", duplicateRSAccQCResultsList.size());
    }
}