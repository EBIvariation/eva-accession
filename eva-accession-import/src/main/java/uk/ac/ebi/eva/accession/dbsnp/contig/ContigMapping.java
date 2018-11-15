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

    private static final String ASSEMBLED_MOLECULE = "assembled-molecule";

    private static final String NOT_AVAILABLE = "na";

    Map<String, ContigSynonyms> sequenceNameToSynonyms = new HashMap<>();

    Map<String, ContigSynonyms> assignedMoleculeToSynonyms = new HashMap<>();

    Map<String, ContigSynonyms> genBankToSynonyms = new HashMap<>();

    Map<String, ContigSynonyms> refSeqToSynonyms = new HashMap<>();

    Map<String, ContigSynonyms> ucscToSynonyms = new HashMap<>();

    private Set<String> nonUniqueAssignedMolecules = new HashSet<>();

    public ContigMapping(String assemblyReportUrl) throws Exception {
        this(new AssemblyReportReader(assemblyReportUrl));
    }

    public ContigMapping(AssemblyReportReader assemblyReportReader) throws Exception {
        populateMaps(assemblyReportReader);
    }

    public ContigMapping(List<ContigSynonyms> contigSynonyms) {
        contigSynonyms.forEach(this::fillContigConventionMaps);
    }

    private void populateMaps(AssemblyReportReader assemblyReportReader) throws Exception {
        ContigSynonyms contigSynonyms;
        while ((contigSynonyms = assemblyReportReader.read()) != null) {
            fillContigConventionMaps(contigSynonyms);
        }
    }

    /**
     * Adds an entry to every map, where the key is a name, and the value is the row where it appears.
     *
     * Take into account:
     * - UCSC and assignedMolecule columns may appear as "na" (not available).
     * - assignedMolecule values may not be unique across rows. Keep only those that have "assembled-molecule" only
     * once in the Sequence-Role column.
     */
    private void fillContigConventionMaps(ContigSynonyms contigSynonyms) {
        normalizeNames(contigSynonyms);

        sequenceNameToSynonyms.put(contigSynonyms.getSequenceName(), contigSynonyms);
        if (contigSynonyms.getAssignedMolecule() != null) {
            assignedMoleculeToSynonyms.put(contigSynonyms.getAssignedMolecule(), contigSynonyms);
        }
        if (contigSynonyms.getGenBank() != null) {
            genBankToSynonyms.put(contigSynonyms.getGenBank(), contigSynonyms);
        }
        if (contigSynonyms.getRefSeq() != null) {
            refSeqToSynonyms.put(contigSynonyms.getRefSeq(), contigSynonyms);
        }
        if (contigSynonyms.getUcsc() != null) {
            ucscToSynonyms.put(contigSynonyms.getUcsc(), contigSynonyms);
        }

    }

    private void normalizeNames(ContigSynonyms contigSynonyms) {
        String assignedMolecule = contigSynonyms.getAssignedMolecule();
        if (NOT_AVAILABLE.equals(assignedMolecule)
                || !ASSEMBLED_MOLECULE.equals(contigSynonyms.getSequenceRole())
                || nonUniqueAssignedMolecules.contains(assignedMolecule)) {
            contigSynonyms.setAssignedMolecule(null);
        } else if (assignedMoleculeToSynonyms.containsKey(assignedMolecule)) {
            nonUniqueAssignedMolecules.add(assignedMolecule);
            ContigSynonyms instancePresentInAllMaps = assignedMoleculeToSynonyms.get(assignedMolecule);
            instancePresentInAllMaps.setAssignedMolecule(null);
            assignedMoleculeToSynonyms.remove(assignedMolecule);
            contigSynonyms.setAssignedMolecule(null);
        }

        if (NOT_AVAILABLE.equals(contigSynonyms.getGenBank())) {
            contigSynonyms.setGenBank(null);
        }
        if (NOT_AVAILABLE.equals(contigSynonyms.getRefSeq())) {
            contigSynonyms.setRefSeq(null);
        }
        if (NOT_AVAILABLE.equals(contigSynonyms.getUcsc())) {
            contigSynonyms.setUcsc(null);
        }
    }

    public ContigSynonyms getContigSynonyms(String contig) {
        ContigSynonyms contigSynonyms;
        if ((contigSynonyms = refSeqToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = genBankToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = assignedMoleculeToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = sequenceNameToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = ucscToSynonyms.get(contig)) != null) {
            return contigSynonyms;
        }
        return null;
    }
}
