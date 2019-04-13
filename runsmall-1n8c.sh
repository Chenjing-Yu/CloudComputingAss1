#!/bin/bash
#SBATCH --time=00:00:02
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --account=comp90024
module load Python/3.6.4-intel-2017.u2-GCC-6.2.0-CUDA9
time mpirun python main.py smallTwitter.json