#!/bin/bash
#sbatch shell script for BPA_firing.R
# 
#all commands that start with SBATCH are used by SLURM for scheduling
#specify number of nodes in SBATCH --nodes instead of mpirun -np
#in this example, we are requesting for 48 CPUs (i.e. 1 master, 47 slaves)
############

#SBATCH --job-name=sarahFindBPA
#SBATCH --partition=shahlab
#SBATCH --qos=normal
#SBATCH --ntasks=13

#SBATCH --mail-type=ALL
#SBATCH --mail-user=spoole@stanford.edu

#to set 2GB per CPU (set either per node OR per CPU)
#SBATCH --mem-per-cpu=2000
#SBATCH --mem=6000

#SBATCH --time=3-00:00

### ensure that openmpi is loaded (if not already loaded)
module load openmpi/default
module load R/3.2.0

### use mpirun to run the R script
### better to specify number of processes in ntasks above than below
mpirun -np 1 R CMD BATCH BPA_firing.R