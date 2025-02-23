package uk.ac.ebi.eva.accession.release.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.release.batch.io.deprecated.DeprecatedVariantAccessionWriter;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DEPRECATED_ACCESSIONS_VARIANT_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_DEPRECATED_RELEASE_WRITER;

@Configuration
@EnableBatchProcessing
public class DeprecatedAccessionReleaseFromDBStepConfiguration {

    @Autowired
    @Qualifier(DEPRECATED_ACCESSIONS_VARIANT_READER)
    ItemReader<Variant> variantReader;

    @Autowired
    @Qualifier(EVA_DEPRECATED_RELEASE_WRITER)
    DeprecatedVariantAccessionWriter accessionWriter;

    @Bean(DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_STEP)
    public Step deprecatedAccessionsReleaseFromDBStep(StepBuilderFactory stepBuilderFactory,
                                                      SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = stepBuilderFactory.get(DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_STEP)
                .<Variant, Variant>chunk(chunkSizeCompletionPolicy)
                .reader(variantReader)
                .writer(accessionWriter)
                .build();
        return step;
    }
}