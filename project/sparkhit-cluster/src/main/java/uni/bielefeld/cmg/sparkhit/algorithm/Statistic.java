package uni.bielefeld.cmg.sparkhit.algorithm;

/**
 * Created by rhinempi on 22.07.2017.
 *
 *       Sparkhit
 *
 * Copyright (c) 2017.
 *       Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Returns an object that can then be used for varies statistic tests.
 * This class can also be used as static methods, without building
 * an instance for each operations.
 *
 * @author  Liren Huang
 * @version %I%, %G%
 * @see
 */
public class Statistic implements ShAlgorithm{
    public Statistic(){
        /**
         * Mainly for statistic methods of different algorithms for HWE calculation
         */
    }

    /**
     * This method can be used to calculate the expected frequencies of each genotype
     * based on Hardy–Weinberg principle
     *
     * @param obs_hets observed value for heterozygous genotype.
     * @param obs_hom1 observed value for Homozygous Dominant genotype.
     * @param obs_hom2 observed value for Homozygous Recessive genotype.
     * @return a string: consists of frequencies of three genotypes.
     */
    public static String calculateHWEP(int obs_hets, int obs_hom1, int obs_hom2) {
        int total = 2*obs_hets + 2*obs_hom1 + 2*obs_hom2;
        int A = obs_hom1*2+obs_hets;
        double AFreq = ((double)A)/total;
        int a = obs_hom2*2+obs_hets;
        double aFreq = ((double)a)/total;

        String head = "HWE expect : ";

        return AFreq*AFreq*total + "\t" + 2*AFreq*aFreq*total + "\t" + aFreq*aFreq*total;
    }

    /**
     * deprecated
     *
     * @param obs_hets
     * @param obs_hom1
     * @param obs_hom2
     * @return
     */
    public static double calculateExactHWEPValue(int obs_hets, int obs_hom1, int obs_hom2) {
        //System.out.println("Starting exact HWE:\t" + obs_hets + "\t" + obs_hom1 + "\t" + obs_hom2);

        int obs_homc = obs_hom1 < obs_hom2 ? obs_hom2 : obs_hom1;
        int obs_homr = obs_hom1 < obs_hom2 ? obs_hom1 : obs_hom2;

        int rare_copies = 2 * obs_homr + obs_hets;
        int l_genotypes = obs_hets + obs_homc + obs_homr;

        if (l_genotypes == 0) {
            return -1;
        }

        double[] het_probs = new double[rare_copies + 1];

        int i;
        for (i = 0; i <= rare_copies; i++) {
            het_probs[i] = 0.0;
        }

        /* start at midpoint */
        int mid = rare_copies * (2 * l_genotypes - rare_copies) / (2 * l_genotypes);

        /* check to ensure that midpoint and rare alleles have same parity */
        if (mid % 2 != rare_copies % 2) {
            mid++;
        }

        int curr_hets = mid;
        int curr_homr = (rare_copies - mid) / 2;
        int curr_homc = l_genotypes - curr_hets - curr_homr;

        het_probs[mid] = 1.0;
        double sum = het_probs[mid];
        for (curr_hets = mid; curr_hets > 1; curr_hets -= 2) {
            het_probs[curr_hets - 2] = het_probs[curr_hets] * curr_hets * (curr_hets - 1.0) / (4.0 * (curr_homr + 1.0) * (curr_homc + 1.0));
            sum += het_probs[curr_hets - 2];
            /* 2 fewer heterozygotes for next iteration -> add one rare, one common homozygote */
            curr_homr++;
            curr_homc++;
        }

        curr_hets = mid;
        curr_homr = (rare_copies - mid) / 2;
        curr_homc = l_genotypes - curr_hets - curr_homr;
        for (curr_hets = mid; curr_hets <= rare_copies - 2; curr_hets += 2) {
            het_probs[curr_hets + 2] = het_probs[curr_hets] * 4.0 * curr_homr * curr_homc / ((curr_hets + 2.0) * (curr_hets + 1.0));
            sum += het_probs[curr_hets + 2];
            curr_homr--;
            curr_homc--;
        }

        for (i = 0; i <= rare_copies; i++) {
            het_probs[i] /= sum;
        }

        double p_hwe = 0.0;
        for (i = 0; i <= rare_copies; i++) {
            if (het_probs[i] <= het_probs[obs_hets]) {
                p_hwe += het_probs[i];
            }
        }

        p_hwe = p_hwe > 1.0 ? 1.0 : p_hwe;

        return p_hwe;
    }

}
