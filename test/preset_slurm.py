import sys
from pathlib import Path
import limes_x as lx
from limes_x.presets.slurm import Run

modules: list[lx.ComputeModule] = []
modules += lx.LoadComputeModules("../../Limes-compute-modules/logistics")
modules += lx.LoadComputeModules("../../Limes-compute-modules/metagenomics")

Run(
    modules = modules,
    reference_folder="../../lx_ref",
    workspace="./cache/test_slurm",
    targets=[
        lx.Item('metagenomic gzipped reads'),
        lx.Item('metagenomic assembly'),
        lx.Item("metagenomic bin"),
        lx.Item("checkm stats"),
        lx.Item('reads taxonomy table'),
        lx.Item('bin taxonomy table'),
        lx.Item('assembly taxonomy table'),
        lx.Item('annotation by metapathways'),
    ],
    given=[
        lx.InputGroup(  
            group_by=(lx.Item("sra accession"), "SRR10140508"), 
            children={
                lx.Item("username"): "Tony",
                lx.Item("metagenomic gzipped reads"): [Path(p) for p in [
                    "/home/tony/workspace/python/Limes-all/Limes-x/test/cache/SRR10140508/SRR10140508_1.fastq.gz",
                    "/home/tony/workspace/python/Limes-all/Limes-x/test/cache/SRR10140508/SRR10140508_2.fastq.gz",
                ]],
            },
        )
    ],
    allocation="alloc_code",
    # continue_from="folder_name",
    # time="48:00:00",
    # name="a test run",
)
