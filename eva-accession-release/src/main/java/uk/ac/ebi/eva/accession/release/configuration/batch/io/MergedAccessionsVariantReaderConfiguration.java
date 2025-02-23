package uk.ac.ebi.eva.accession.release.configuration.batch.io;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.eva.accession.release.batch.io.MergedAccessionsVariantReader;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.commons.batch.io.UnwindingItemStreamReader;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.MERGED_ACCESSIONS_VARIANT_READER;

@Configuration
public class MergedAccessionsVariantReaderConfiguration {
    @Bean(MERGED_ACCESSIONS_VARIANT_READER)
    @StepScope
    public ItemStreamReader<Variant> mergedAccessionsVariantUnwindingReader(MongoTemplate mongoTemplate, InputParameters parameters) {
        return new UnwindingItemStreamReader<>(
                new MergedAccessionsVariantReader(mongoTemplate, parameters.getRsAccFile(),
                        parameters.getAssemblyAccession(), parameters.getTaxonomyAccession(), parameters.getChunkSize()));
    }
}
