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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContigMapping {

    private Map<String, ContigSynonyms> sequenceNameToSynonyms = new HashMap<>();

    private Map<String, ContigSynonyms> assignedMoleculeToSynonyms = new HashMap<>();

    private boolean assignedMoleculeHasDuplicates = false;

    private Map<String, ContigSynonyms> genBankToSynonyms = new HashMap<>();

    private Map<String, ContigSynonyms> refSeqToSynonyms = new HashMap<>();

    private Map<String, ContigSynonyms> ucscToSynonyms = new HashMap<>();

    private static final String CHROMOSOME_PATTERN = "(chromosome|chrom|chr|ch)(.+)";

    private static final Pattern PATTERN = Pattern.compile(CHROMOSOME_PATTERN, Pattern.CASE_INSENSITIVE);

    private static final String NOT_AVAILABLE = "na";

    public ContigMapping(String assemblyReportUrl) throws Exception {
        this(new AssemblyReportReader(assemblyReportUrl));
    }

    public ContigMapping(AssemblyReportReader assemblyReportReader) throws Exception {
        populateMaps(assemblyReportReader);
    }

    public ContigMapping(List<ContigSynonyms> contigSynonyms) {
        contigSynonyms.forEach(this::fillContigConventionMaps);
        if (assignedMoleculeHasDuplicates) {
            removeAssignedMolecule();
        }
    }

    private void removeAssignedMolecule() {
        assignedMoleculeToSynonyms.clear();
        sequenceNameToSynonyms.forEach((key, contigSynonym) -> contigSynonym.setAssignedMolecule(null));
        ucscToSynonyms.forEach((key, contigSynonym) -> contigSynonym.setAssignedMolecule(null));
        genBankToSynonyms.forEach((key, contigSynonym) -> contigSynonym.setAssignedMolecule(null));
        refSeqToSynonyms.forEach((key, contigSynonym) -> contigSynonym.setAssignedMolecule(null));
    }

    private void populateMaps(AssemblyReportReader assemblyReportReader) throws Exception {
        ContigSynonyms contigSynonyms;
        while ((contigSynonyms = assemblyReportReader.read()) != null) {
            fillContigConventionMaps(contigSynonyms);
        }
        if (assignedMoleculeHasDuplicates) {
            removeAssignedMolecule();
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
        contigSynonyms.setSequenceName(removePrefix(contigSynonyms.getSequenceName()));
        if (contigSynonyms.getAssignedMolecule().equals(NOT_AVAILABLE)) {
            contigSynonyms.setAssignedMolecule(null);
        }
        if (contigSynonyms.getUcsc().equals(NOT_AVAILABLE)) {
            contigSynonyms.setUcsc(null);
        } else {
            contigSynonyms.setUcsc(removePrefix(contigSynonyms.getUcsc()));
        }

        sequenceNameToSynonyms.put(contigSynonyms.getSequenceName(), contigSynonyms);
        genBankToSynonyms.put(contigSynonyms.getGenBank(), contigSynonyms);
        refSeqToSynonyms.put(contigSynonyms.getRefSeq(), contigSynonyms);

        if (contigSynonyms.getUcsc() != null) {
            ucscToSynonyms.put(contigSynonyms.getUcsc(), contigSynonyms);
        }

        if (!assignedMoleculeHasDuplicates && contigSynonyms.getAssignedMolecule() != null) {
            ContigSynonyms previousValue = assignedMoleculeToSynonyms.putIfAbsent(contigSynonyms.getAssignedMolecule(),
                                                                                  contigSynonyms);
            if (previousValue != null) {
                assignedMoleculeHasDuplicates = true;
            }
        }
    }

    private String removePrefix(String contig) {
        Matcher matcher = PATTERN.matcher(contig);
        String contigNoPrefix = contig;
        if (matcher.matches()) {
            contigNoPrefix = matcher.group(2);
        }
        return contigNoPrefix;
    }

    public ContigSynonyms getContigSynonyms(String contig) {
        String contigNoPrefix = removePrefix(contig);
        ContigSynonyms contigSynonyms;
        if ((contigSynonyms = sequenceNameToSynonyms.get(contigNoPrefix)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = genBankToSynonyms.get(contigNoPrefix)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = refSeqToSynonyms.get(contigNoPrefix)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = ucscToSynonyms.get(contigNoPrefix)) != null) {
            return contigSynonyms;
        }
        if (!assignedMoleculeHasDuplicates
                && (contigSynonyms = assignedMoleculeToSynonyms.get(contigNoPrefix)) != null) {
            return contigSynonyms;
        }
        return null;
    }
}
