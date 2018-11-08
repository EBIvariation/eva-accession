package uk.ac.ebi.eva.accession.release.parameters;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

public class InputParameters {

    private String assemblyAccession;

    public JobParameters toJobParameters() {
        return new JobParametersBuilder()
                .addString("assemblyAccession", assemblyAccession)
                .toJobParameters();
    }

    public String getAssemblyAccession() {
        return assemblyAccession;
    }
}
