package uk.ac.ebi.eva.accession.release.parameters;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

public class InputParameters {

    private String assemblyAccession;

    private String fasta;

    private String assemblyReportUrl;

    private String outputVcf;

    private boolean forceRestart;

    private int chunkSize;

    public JobParameters toJobParameters() {
        return new JobParametersBuilder()
                .addString("assemblyAccession", assemblyAccession)
                .addString("fasta", fasta)
                .addString("assemblyReportUrl", assemblyReportUrl)
                .addString("outputVcf", outputVcf)
                .toJobParameters();
    }

    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    public void setAssemblyAccession(String assemblyAccession) {
        this.assemblyAccession = assemblyAccession;
    }

    public String getFasta() {
        return fasta;
    }

    public void setFasta(String fasta) {
        this.fasta = fasta;
    }

    public String getAssemblyReportUrl() {
        return assemblyReportUrl;
    }

    public void setAssemblyReportUrl(String assemblyReportUrl) {
        this.assemblyReportUrl = assemblyReportUrl;
    }

    public String getOutputVcf() {
        return outputVcf;
    }

    public void setOutputVcf(String outputVcf) {
        this.outputVcf = outputVcf;
    }

    public boolean isForceRestart() {
        return forceRestart;
    }

    public void setForceRestart(boolean forceRestart) {
        this.forceRestart = forceRestart;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }
}
