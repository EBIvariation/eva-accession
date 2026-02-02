package uk.ac.ebi.eva.accession.pipeline.batch.io;

import gherkin.deps.com.google.gson.Gson;
import gherkin.deps.com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class DuplicateSSAccQCWriter implements ItemStreamWriter<List<DuplicateSSAccQCResult>> {
    private static final Logger logger = LoggerFactory.getLogger(DuplicateSSAccQCWriter.class);
    private String duplicateSSAccFile;
    private BufferedWriter writer;
    private final Gson gson = new GsonBuilder().create();

    public DuplicateSSAccQCWriter(String duplicateSSAccFile) {
        this.duplicateSSAccFile = duplicateSSAccFile;
    }

    @Override
    public void open(org.springframework.batch.item.ExecutionContext executionContext) throws ItemStreamException {
        try {
            writer = new BufferedWriter(new FileWriter(duplicateSSAccFile, true));
        } catch (IOException e) {
            throw new ItemStreamException("Failed to open the file (" + duplicateSSAccFile + ") to write duplicate SS accessions", e);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            throw new ItemStreamException("Failed to close file: " + duplicateSSAccFile, e);
        }
    }

    @Override
    public void write(List<? extends List<DuplicateSSAccQCResult>> listOfDuplicateSSAccQCResultLists) throws Exception {
        for (List<DuplicateSSAccQCResult> duplicateSSAccQCResultList : listOfDuplicateSSAccQCResultLists) {
            if (duplicateSSAccQCResultList != null && !duplicateSSAccQCResultList.isEmpty()) {
                appendToFile(duplicateSSAccQCResultList);
            }
        }
    }

    private void appendToFile(List<DuplicateSSAccQCResult> duplicateSSAccQCResultList) throws IOException {
        for (DuplicateSSAccQCResult duplicateSSAccQCResult : duplicateSSAccQCResultList) {
            writer.write(duplicateSSAccQCResult.getSveAccession().toString() + " " + gson.toJson(duplicateSSAccQCResult));
            writer.newLine();
        }
        logger.warn("Appended {} duplicate SS IDs to the file", duplicateSSAccQCResultList.size());
    }
}