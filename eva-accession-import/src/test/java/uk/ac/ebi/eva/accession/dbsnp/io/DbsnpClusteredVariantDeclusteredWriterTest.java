package uk.ac.ebi.eva.accession.dbsnp.io;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.summary.DbsnpClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Collections;
import java.util.function.Function;

import static uk.ac.ebi.eva.accession.dbsnp.io.DbsnpClusteredVariantDeclusteredWriter.DBSNP_CLUSTERED_VARIANT_DECLUSTERED;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:test-variants-writer.properties")
@ContextConfiguration(classes = {MongoConfiguration.class})
public class DbsnpClusteredVariantDeclusteredWriterTest {

    private static final int TAXONOMY = 3880;

    private static final long EXPECTED_ACCESSION = 10000000000L;

    private static final int START = 100;

    private static final VariantType VARIANT_TYPE = VariantType.SNV;

    private static final Boolean VALIDATED = false;

    private ImportCounts importCounts;

    private DbsnpClusteredVariantDeclusteredWriter writer;

    private Function<IClusteredVariant, String> hashingFunction;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Before
    public void setUp() throws Exception {
        importCounts = new ImportCounts();
        writer = new DbsnpClusteredVariantDeclusteredWriter(mongoTemplate);
        hashingFunction = new DbsnpClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        mongoTemplate.dropCollection(DBSNP_CLUSTERED_VARIANT_DECLUSTERED);
    }

    @Test
    public void writeDeclusteredRs() {
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY, "contig", START,
                                                                 VARIANT_TYPE, VALIDATED);
        DbsnpClusteredVariantEntity variant = new DbsnpClusteredVariantEntity(EXPECTED_ACCESSION,
                                                                              hashingFunction.apply(clusteredVariant),
                                                                              clusteredVariant);
        writer.write(Collections.singletonList(variant));

    }

}
