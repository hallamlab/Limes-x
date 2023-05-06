import sys
from pathlib import Path
import limes_x as lx
from limes_x.presets.slurm import Run

modules = []
modules += lx.LoadComputeModules("../../Limes-compute-modules/logistics")
modules += lx.LoadComputeModules("../../Limes-compute-modules/metagenomics")

Run(
    modules = modules,
    reference_folder="../../lx_ref",
    workspace="./cache/test_workspace",
    targets=[
        lx.Item('metagenomic gzipped reads'),
        lx.Item('metagenomic assembly'),
        lx.Item("metagenomic bin"),
        lx.Item("checkm stats"),
        lx.Item('bin taxonomy table'),
        lx.Item('assembly taxonomy table'),
        lx.Item('genomic annotation'),
    ],
    given=[
        lx.InputGroup(  
            group_by=(lx.Item("sra accession"), "SRR10140508"), 
            children={
                lx.Item("username"): "Steven",
            },
        )
    ],
    allocation="alloc_code",
    # continue_from="folder_name",
    # time="48:00:00",
    # name="a test run",
)
