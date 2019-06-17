/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.dbsnp.model;

public class CoordinatesPresence {

    private String chromosome;

    private boolean chromosomeStartPresent;

    private String contig;
    // no contigStartPresent because it will always be present

    public CoordinatesPresence(String chromosome, boolean chromosomeStartPresent, String contig) {
        this.chromosome = chromosome;
        this.chromosomeStartPresent = chromosomeStartPresent;
        this.contig = contig;
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public boolean isChromosomeStartPresent() {
        return chromosomeStartPresent;
    }

    public void setChromosomeStartPresent(boolean chromosomeStartPresent) {
        this.chromosomeStartPresent = chromosomeStartPresent;
    }

    public String getContig() {
        return contig;
    }

    public void setContig(String contig) {
        this.contig = contig;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CoordinatesPresence)) {
            return false;
        }

        CoordinatesPresence presence = (CoordinatesPresence) o;

        if (chromosomeStartPresent != presence.chromosomeStartPresent) {
            return false;
        }
        if (chromosome != null ? !chromosome.equals(presence.chromosome) : presence.chromosome != null) {
            return false;
        }
        return contig != null ? contig.equals(presence.contig) : presence.contig == null;

    }

    @Override
    public int hashCode() {
        int result = chromosome != null ? chromosome.hashCode() : 0;
        result = 31 * result + (chromosomeStartPresent ? 1 : 0);
        result = 31 * result + (contig != null ? contig.hashCode() : 0);
        return result;
    }
}
