#!/bin/bash
#SBATCH --account=share-ie-idi
#SBATCH --partition=CPUQ
#SBATCH --time=5:00:00
#SBATCH --nodes=1             # 1 compute nodes
#SBATCH --cpus-per-task=8     # 2 CPU cores
#SBATCH --mem=128G             # GB memory
#SBATCH --output=confident_new_rule/output_confident_new_rule_test_10_min_K-feats%j.txt    # Log file
#SBATCH --mail-user=trulsc00@gmail.com
#SBATCH --mail-type=ALL

# Go to submission folder
cd ~/Label-ais-ers/Master-prework/label_ais_pts_w_ers

# Load Python
module purge
module load Python/3.11.5-GCCcore-13.2.0

# Activate venv
source ~/venv/myenv/bin/activate

# Run your script
#python label_pts.py
python -u confident_negs_NEW_scale_fast.py