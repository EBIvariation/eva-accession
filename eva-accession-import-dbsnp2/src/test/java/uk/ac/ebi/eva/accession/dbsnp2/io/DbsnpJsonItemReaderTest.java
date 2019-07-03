package uk.ac.ebi.eva.accession.dbsnp2.io;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.test.StepScopeTestExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.ImportDbsnpJsonVariantsReaderConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.parameters.InputParameters;
import uk.ac.ebi.eva.accession.dbsnp2.test.BatchTestConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.DBSNP_JSON_VARIANT_READER;

@TestPropertySource({"classpath:application.properties"})
@ContextConfiguration(classes = {InputParameters.class, BatchTestConfiguration.class,
        ImportDbsnpJsonVariantsReaderConfiguration.class})
@TestExecutionListeners({DependencyInjectionTestExecutionListener.class, StepScopeTestExecutionListener.class})
@RunWith(SpringJUnit4ClassRunner.class)
@EnableBatchProcessing
public class DbsnpJsonItemReaderTest {

    @Autowired
    @Qualifier(DBSNP_JSON_VARIANT_READER)
    @StepScope
    private FlatFileItemReader<JsonNode> reader;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void readChrYDbsnpJsonVariants() throws Exception {
        reader.setResource(new BzipLazyResource(new File("src/test/resources/input-files/chrY50.json.bz2")));
        reader.open(new ExecutionContext());
        List<JsonNode> variants = readAll(reader);
        assertEquals(50, variants.size());
    }

    @Test
    public void readWrongSource() throws Exception {
        thrown.expect(ItemStreamException.class);
        reader.setResource(new BzipLazyResource(new File("INVALID_DIRECTORY")));
        reader.open(new ExecutionContext());
        readAll(reader);
    }

    private List<JsonNode> readAll(FlatFileItemReader<JsonNode> reader) throws Exception {
        List<JsonNode> variants = new ArrayList<>();
        JsonNode variant;
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }
        return variants;
    }
}
