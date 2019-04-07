#!/bin/bash
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1
#module load Python/3.6.4-intel-2017.u2-GCC-6.2.0-CUDA9
#time mpirun -n 8 main.py
time mpiexec -np 8 python main.py
