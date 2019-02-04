package uk.ac.ebi.eva.accession.release.io;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class ContigWriter implements ItemStreamWriter<String> {

    private final File output;

    private PrintWriter printWriter;

    public ContigWriter(File output) {
        this.output = output;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            printWriter = new PrintWriter(new FileWriter(this.output));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        printWriter.close();
    }

    @Override
    public void write(List<? extends String> contigs) throws Exception {
        for (String contig : contigs) {
            printWriter.println(contig);
        }
    }
}
