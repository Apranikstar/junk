import sys
import os
import argparse
import subprocess
from subprocess import Popen, PIPE
from datetime import date
import time
import concurrent.futures


def get_process_from_sample(sample):
    import re
    match = re.search(r"mgp8_pp_(.+?)_(?:HT|Q)", sample)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Could not extract process from sample name: {sample}")


# ________________________________________________________________________________
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--indir",
        help="path input directory",
        default="/eos/experiment/fcc/hh/generation/DelphesEvents/fcc_v07/II/",
    )
    parser.add_argument(
        "--outdir",
        help="path output directory",
        default=os.path.join(os.getcwd(), "output"),
    )

    parser.add_argument("--sample", help="sample name", default="mgp8_pp_tt_HT_2000_100000_5f_84TeV")
    parser.add_argument("--ncpus", help="number of cpus", type=int, default=64)
    parser.add_argument("--opt", help="option 1: run stage 1, 2: run stage 2, 3: all 4: clean", default="3")

    args = parser.parse_args()
    indir = args.indir
    outdir = args.outdir
    ncpus = args.ncpus
    sample = args.sample
    opt = args.opt

    ## qq is merge of uu/dd
    #flavors = ["bb", "cc", "ss", "gg", "qq", "tautau"]
    flavor_to_process = {
    "jj": "jj",  # THIS is the fix!
    "tt" : "tt"
    }

    process = get_process_from_sample(sample)
    if process not in flavor_to_process:
        raise ValueError(f"Process {process} not recognized in flavor_to_process mapping.")
    f = flavor_to_process[process]

    outtmpdir = os.path.join(os.getcwd(), "tmp")    
    #outtmpdir = "/tmp/selvaggi/data/stage_all"
    os.system("rm -rf {}".format(outtmpdir))
    os.system("mkdir -p {}".format(outtmpdir))
    os.system("mkdir -p {}".format(outdir))

    ## fill name of stage1 files
    stage1_files = dict()
    #for f in flavors:
        #stage1_files[f] = "{}/stage1_{}.root".format(outtmpdir, f)
    stage1_file = f"{outtmpdir}/stage1_{f}.root"
    edm_files = ""

    ### run stage 1
    if opt in ["1", "3"]:
        sample_f = sample.replace("XX", f)   # (keep this if your sample has XX placeholder)
        edm_files = f"{indir}/{sample_f}/*.root"
        cmd_stage1 = (
            f"fccanalysis run examples/FCCee/weaver/stage1_gen.py "
            f"--output {stage1_file} --files-list {edm_files} --ncpus {ncpus}"
        )
        print("\nRunning stage 1:\n", cmd_stage1, "\n")
        os.system(cmd_stage1)

    ### run stage 2
    if opt in ["2", "3"]:
        nevents = count_events(stage1_file)
        nevents_per_thread = max(1, int(nevents / ncpus))

        stage2_files = {}
        stage2_final_file = f"{outtmpdir}/stage2_H{f}.root"
        stage2_wild_files = f"{outtmpdir}/stage2_H{f}_*.root"
        hadd_cmd = f"hadd -f {stage2_final_file} {stage2_wild_files}"

        commands_stage2 = []
        for i in range(ncpus):
            stage2_files[i] = f"{outtmpdir}/stage2_H{f}_{i}.root"
            nstart = i * nevents_per_thread
            nend = nstart + nevents_per_thread

            cmd_stage2 = (
                f"python examples/FCCee/weaver/stage2.py "
                f"{stage1_file} {stage2_files[i]} {nstart} {nend}"
            )
            commands_stage2.append(cmd_stage2)

        # parallel execution
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=ncpus)
        future_to_command = {executor.submit(run_command, cmd): cmd for cmd in commands_stage2}
        concurrent.futures.wait(future_to_command)

        # merge stage2 outputs
        print(f"\nCollecting stage 2 files into: {stage2_final_file}")
        os.system(hadd_cmd)
        print("Copying final file to output dir...")
        os.system(f"cp {stage2_final_file} {outdir}")
        print("Cleaning tmp files...")
        os.system(f"rm -rf {stage2_final_file} {stage1_file} {stage2_wild_files}")
        print("Done.")


# ________________________________________________________________________________
def run_command(command):
    print(f"running command: {command}")
    os.system(command)


# ________________________________________________________________________________
def count_events(file, tree_name="events"):
    import ROOT
    root_file = ROOT.TFile.Open(file)
    tree = root_file.Get(tree_name)
    return tree.GetEntries()


# ________________________________________________________________________________
if __name__ == "__main__":
    main()
