package uni.bielefeld.cmg.sparkhit.algorithm;

/**
 * Created by Liren Huang on 13/01/16.
 *
 *     SparkHit
 *
 * Copyright (c) 2015-2015:
 * Liren Huang     <huanglr at cebitec.uni-bielefeld.de>
 *
 * SparkHit is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOU
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 */
public class Statistic implements ShAlgorithm{
    public Statistic(){
        /**
         * Mainly for statistic methods of different algorithms for HWE calculation
         */
    }

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
