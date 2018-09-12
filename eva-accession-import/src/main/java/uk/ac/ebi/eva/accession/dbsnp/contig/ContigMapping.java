/*
 *
 * Copyright 2018 EMBL - European Bioinformatics Institute
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
 *
 */
package uk.ac.ebi.eva.accession.dbsnp.contig;

import uk.ac.ebi.eva.accession.dbsnp.io.AssemblyReportReader;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ContigMapping {

    private Map<String, ContigSynonyms> sequenceNameToSynonyms = new HashMap<>();

    private Map<String, ContigSynonyms> assignedMoleculeToSynonyms = new HashMap<>();

    private Set<String> assignedMoleculeDuplicates = new HashSet<>();

    private Map<String, ContigSynonyms> genBankToSynonyms = new HashMap<>();

    private Map<String, ContigSynonyms> refSeqToSynonyms = new HashMap<>();

    private Map<String, ContigSynonyms> ucscToSynonyms = new HashMap<>();

    private static final String NOT_AVAILABLE = "na";

    public ContigMapping(String assemblyReportUrl) throws Exception {
        this(new AssemblyReportReader(assemblyReportUrl));
    }

    public ContigMapping(AssemblyReportReader assemblyReportReader) throws Exception {
        populateMaps(assemblyReportReader);
    }

    public ContigMapping(List<ContigSynonyms> contigSynonyms) {
        contigSynonyms.forEach(this::fillContigConventionMaps);
        if (!assignedMoleculeDuplicates.isEmpty()) {
            removeDuplicatedAssignedMolecule();
        }
    }

    private void removeDuplicatedAssignedMolecule() {
        assignedMoleculeDuplicates.forEach(assignedMoleculeToSynonyms::remove);

        sequenceNameToSynonyms.forEach(this::removeAssignedMoleculeIfDuplicated);
        ucscToSynonyms.forEach(this::removeAssignedMoleculeIfDuplicated);
        genBankToSynonyms.forEach(this::removeAssignedMoleculeIfDuplicated);
        refSeqToSynonyms.forEach(this::removeAssignedMoleculeIfDuplicated);
    }

    private void removeAssignedMoleculeIfDuplicated(String key, ContigSynonyms contigSynonym) {
        if (assignedMoleculeDuplicates.contains(contigSynonym.getAssignedMolecule())) {
            contigSynonym.setAssignedMolecule(null);
        }
    }

    private void populateMaps(AssemblyReportReader assemblyReportReader) throws Exception {
        ContigSynonyms contigSynonyms;
        while ((contigSynonyms = assemblyReportReader.read()) != null) {
            fillContigConventionMaps(contigSynonyms);
        }
        if (!assignedMoleculeDuplicates.isEmpty()) {
            removeDuplicatedAssignedMolecule();
        }
    }

    /**
     * Adds an entry to every map, where the key is a name, and the value is the row where it appears.
     *
     * Take into account:
     * - sequenceName and UCSC columns may have prefixes that we have to remove.
     * - UCSC and assignedMolecule columns may appear as "na" (not available).
     * - assignedMolecule values may not be unique across rows. If that's the case, don't include assignedMolecule in
     * the maps.
     */
    private void fillContigConventionMaps(ContigSynonyms contigSynonyms) {
        normalizeNames(contigSynonyms);

        sequenceNameToSynonyms.put(contigSynonyms.getSequenceName(), contigSynonyms);
        genBankToSynonyms.put(contigSynonyms.getGenBank(), contigSynonyms);
        refSeqToSynonyms.put(contigSynonyms.getRefSeq(), contigSynonyms);

        if (contigSynonyms.getUcsc() != null) {
            ucscToSynonyms.put(contigSynonyms.getUcsc(), contigSynonyms);
        }

        if (contigSynonyms.getAssignedMolecule() != null) {
            ContigSynonyms previousValue = assignedMoleculeToSynonyms.putIfAbsent(contigSynonyms.getAssignedMolecule(),
                                                                                  contigSynonyms);
            if (previousValue != null) {
                assignedMoleculeDuplicates.add(contigSynonyms.getAssignedMolecule());
            }
        }
    }

    private void normalizeNames(ContigSynonyms contigSynonyms) {
        contigSynonyms.setSequenceName(contigSynonyms.getSequenceName());
        if (contigSynonyms.getAssignedMolecule().equals(NOT_AVAILABLE)) {
            contigSynonyms.setAssignedMolecule(null);
        }
        if (contigSynonyms.getGenBank().equals(NOT_AVAILABLE)) {
            contigSynonyms.setGenBank(null);
        }
        if (contigSynonyms.getRefSeq().equals(NOT_AVAILABLE)) {
            contigSynonyms.setRefSeq(null);
        }
        if (contigSynonyms.getUcsc().equals(NOT_AVAILABLE)) {
            contigSynonyms.setUcsc(null);
        } else {
            contigSynonyms.setUcsc(contigSynonyms.getUcsc());
        }
    }

    public ContigSynonyms getContigSynonyms(String contig) {
        ContigSynonyms contigSynonyms;
        if ((contigSynonyms = sequenceNameToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = genBankToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = refSeqToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = ucscToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = assignedMoleculeToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        return null;
    }
}
