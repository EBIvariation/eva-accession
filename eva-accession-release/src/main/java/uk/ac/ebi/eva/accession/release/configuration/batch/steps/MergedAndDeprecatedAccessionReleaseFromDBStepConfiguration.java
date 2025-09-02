package uk.ac.ebi.eva.accession.release.configuration.batch.steps;

import htsjdk.variant.variantcontext.VariantContext;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_MERGED_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.MERGED_AND_DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.MERGED_AND_DEPRECATED_ACCESSIONS_VARIANT_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_PROCESSOR;

@Configuration
@EnableBatchProcessing
public class MergedAndDeprecatedAccessionReleaseFromDBStepConfiguration {

    @Autowired
    @Qualifier(MERGED_AND_DEPRECATED_ACCESSIONS_VARIANT_READER)
    ItemReader<Variant> variantReader;

    @Autowired
    @Qualifier(RELEASE_PROCESSOR)
    ItemProcessor<Variant, VariantContext> variantProcessor;

    @Autowired
    @Qualifier(EVA_MERGED_RELEASE_WRITER)
    ItemStreamWriter<VariantContext> accessionWriter;

    @Bean(MERGED_AND_DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_STEP)
    public Step mergedAndDeprecatedAccessionsReleaseFromDBStep(StepBuilderFactory stepBuilderFactory,
                                                               SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = stepBuilderFactory.get(MERGED_AND_DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_STEP)
                .<Variant, VariantContext>chunk(chunkSizeCompletionPolicy)
                .reader(variantReader)
                .processor(variantProcessor)
                .writer(accessionWriter)
                .build();
        return step;
    }
}