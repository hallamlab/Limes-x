import limes_x as lx
from limes_x import Item

modules = []
modules += lx.LoadComputeModules("../../Limes-compute-modules/logistics")
modules += lx.LoadComputeModules("../../Limes-compute-modules/metagenomics")
wf = lx.Workflow(
    compute_modules=modules,
    reference_folder="../../lx_ref",
)

wf.Run(
    workspace="./cache/test_local",
    targets=[
        Item('metagenomic gzipped reads'),
        Item('metagenomic assembly'),
        Item("metagenomic bin"),
        Item("checkm stats"),
        Item('bin taxonomy table'),
        Item('assembly taxonomy table'),
        Item('genomic annotation'),
    ],
    given=[
        lx.InputGroup(  
            group_by=(Item("sra accession"), "SRR19573024"), 
            children={Item("username"): "tony"}, # use "whoami" in bash
        )
    ],
    executor=lx.Executor(),
)
