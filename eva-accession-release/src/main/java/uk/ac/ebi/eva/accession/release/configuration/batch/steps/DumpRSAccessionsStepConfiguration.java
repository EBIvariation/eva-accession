package uk.ac.ebi.eva.accession.release.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.release.batch.io.DumpRSAccessionsInFile;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_ACTIVE_ACCESSIONS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_RS_ACCESSIONS_IN_FILE;

@Configuration
@EnableBatchProcessing
public class DumpRSAccessionsStepConfiguration {

    @Autowired
    @Qualifier(DUMP_RS_ACCESSIONS_IN_FILE)
    private DumpRSAccessionsInFile dumpRSAccessionsInFile;

    @Bean(DUMP_ACTIVE_ACCESSIONS_STEP)
    public Step dumpActiveAccessionStep(StepBuilderFactory stepBuilderFactory, InputParameters inputParameters) {
        return stepBuilderFactory.get(DUMP_ACTIVE_ACCESSIONS_STEP)
                .tasklet((contribution, chunkContext) -> {
                    dumpRSAccessionsInFile.dumpAccessions(DumpRSAccessionsInFile.RSDumpType.ACTIVE,
                            inputParameters.getAssemblyAccession());
                    return null;
                })
                .build();
    }

    @Bean(DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_STEP)
    public Step dumpMergedAndDeprecatedAccessionStep(StepBuilderFactory stepBuilderFactory, InputParameters inputParameters) {
        return stepBuilderFactory.get(DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_STEP)
                .tasklet((contribution, chunkContext) -> {
                    dumpRSAccessionsInFile.dumpAccessions(DumpRSAccessionsInFile.RSDumpType.MERGED_AND_DEPRECATED,
                            inputParameters.getAssemblyAccession());
                    return null;
                })
                .build();
    }
}