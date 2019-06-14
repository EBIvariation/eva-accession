package uk.ac.ebi.eva.accession.dbsnp.configuration;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpJson;
import uk.ac.ebi.eva.commons.core.models.IVariant;

import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.DBSNP_JSON_VARIANT_PROCESSOR;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.DBSNP_JSON_VARIANT_READER;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.DBSNP_JSON_VARIANT_WRITER;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.IMPORT_DBSNP_JSON_VARIANTS_STEP;

/**
 * Step configuration for Dbsnp Json import flow
 */
@Configuration
@EnableBatchProcessing
public class ImportDbsnpJsonVariantsStepConfiguration {

    @Autowired
    @Qualifier(DBSNP_JSON_VARIANT_READER)
    private ItemReader<DbsnpJson> variantReader;

    @Autowired
    @Qualifier(DBSNP_JSON_VARIANT_PROCESSOR)
    private ItemProcessor<DbsnpJson, IVariant> variantProcessor;

    @Autowired
    @Qualifier(DBSNP_JSON_VARIANT_WRITER)
    private ItemWriter<IVariant> variantWriter;

    @Bean(IMPORT_DBSNP_JSON_VARIANTS_STEP)
    public Step importDbsnpJsonVariantsStep(StepBuilderFactory stepBuilderFactory,
                                          SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = stepBuilderFactory.get(IMPORT_DBSNP_JSON_VARIANTS_STEP)
            .<DbsnpJson, IVariant>chunk(chunkSizeCompletionPolicy)
            .reader(variantReader)
            .processor(variantProcessor)
            .writer(variantWriter)
            .build();
        return step;
    }
}
