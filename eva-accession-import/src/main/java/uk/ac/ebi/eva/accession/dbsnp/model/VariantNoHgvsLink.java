/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.dbsnp.model;

import java.sql.Date;

public class VariantNoHgvsLink {

    private String alleles;

    private String assembly;

    private String batchHandle;

    private String batchName;

    private String chromosome;

    private int chromosomeStart;

    private String contigName;

    private Orientation contigOrientation;

    private int contigStart;

    private boolean frequencyExists;

    private boolean genotypExists;

    private int loadOrder;

    private String reference;

    private Date createTime;

    private int taxonomyId;

    // TODO: sort the fields in a logical order, or just leave them in alphabetical order?
    public VariantNoHgvsLink(String alleles, String assembly, String batchHandle, String batchName,
                             String chromosome, int chromosomeStart, String contigName, Orientation contigOrientation,
                             int contigStart, boolean frequencyExists, boolean genotypExists, int loadOrder,
                             String reference, Date createTime, int taxonomyId) {
        this.alleles = alleles;
        this.assembly = assembly;
        this.batchHandle = batchHandle;
        this.batchName = batchName;
        this.chromosome = chromosome;
        this.chromosomeStart = chromosomeStart;
        this.contigName = contigName;
        this.contigOrientation = contigOrientation;
        this.contigStart = contigStart;
        this.frequencyExists = frequencyExists;
        this.genotypExists = genotypExists;
        this.loadOrder = loadOrder;
        this.reference = reference;
        this.createTime = createTime;
        this.taxonomyId = taxonomyId;
    }
}
