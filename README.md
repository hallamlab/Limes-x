<img src="https://raw.githubusercontent.com/hallamlab/Limes-x/main/docs/images/Limes-x_logo.svg" alt="Limes-x"/>

### *Workflows on demand!*

# For the impatient- Local execution

### **Dependencies**
- Python
- Singularity
- Git (to setup existing modules)
- Snakemake (to setup existing modules)

### **Setup**

```bash
#!/bin/bash
pip install limes-x

git clone https://github.com/hallamlab/Limes-compute-modules.git
python ./Limes-compute-module/setup_modules.py ./lx_ref
```

### **Run**

```python
#!/bin/python3.10
import limes_x as lx

modules = []
modules += lx.LoadComputeModules("path/to/Limes-compute-modules/logistics") #Change this based on your file path
modules += lx.LoadComputeModules("path/to/Limes-compute-modules/metagenomics") #Change this based on your file path

wf = lx.Workflow(
    compute_modules=modules,
    reference_folder="path/to/lx_ref", #Change this based on your file path
)

wf.Run(
    workspace="path/to/output/directory", #Change this based on your file path
    targets=[
        lx.Item('metagenomic gzipped reads'),
        lx.Item('metagenomic assembly'),
        lx.Item("metagenomic bin"),
        lx.Item('bin taxonomy table'),
        lx.Item('assembly taxonomy table'),
        lx.Item('genomic annotation'),
    ],
    given=[
        lx.InputGroup(  
            group_by=(lx.Item("sra accession"), "SRR"), 
            children={lx.Item("username"): "user"}, # use "whoami" in bash
        )
    ],
    executor=lx.Executor(),
    max_concurrent=1,
)
```

# Dependencies

- Anaconda (optional, but recommended)
    - [faster version (Mamba)](https://mamba.readthedocs.io/en/latest/installation.html); Use "mamba" instead of "conda" below
    - [Anaconda](https://docs.anaconda.com/free/anaconda/install/index.html)
- Python
    - >**NOTE:** Most compute servers and linux distributions will have python installed already
    - [install with Anaconda](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
        - `conda create -n test_env python=3.10`
        - feel free to pick a better name than "test_env"
- Singularity
    - >**NOTE:** Most compute servers will have singularity installed already
    - [install with Anaconda](https://anaconda.org/conda-forge/singularity)
        - activate the conda environment: `conda activate test_env`
        - `conda install singularity`
    - [manual install](https://docs.sylabs.io/guides/latest/user-guide/quick_start.html#quick-installation-steps)
- Git
    - you may already have git
        - use `git --version` in the console to find out
    - [otherwise, here's a tutorial](https://github.com/git-guides/install-git)

# Setup

> **NOTE:** We are working on a conda package. Meanwhile, this will work in a conda environment with python installed.
```
pip install limes-x
```

# Running workflows

### **Compute Modules**

Limes-x encapsulates the complexity of workflows by surfacing a declarative syntax that allows you to focus on *what* you want and worry less about *how* to achieve it. This is made possible by compute modules that provide conversions between datatypes such as changing the format of an image or assembling a metagenome from Illumina sequences.

<img src="https://raw.githubusercontent.com/Tony-xy-Liu/Limes-x/main/docs/images/wf_diagram.svg" alt="workflow diagram"/>

Limes-x finds the set of compute modules required to convert the given inputs to the desired outputs. This set of modules is then joined together into an execution-ready workflow. 

[A list of currently available compute modules can be found at this repository](https://github.com/Tony-xy-Liu/Limes-compute-modules)
<br>
Use the `setup_modules` script to install each module's dependencies and reference databases using Singularity and Snakemake. 
```bash
git clone https://github.com/hallamlab/Limes-compute-modules.git
python ./Limes-compute-module/setup_modules.py ./lx_ref
```

### **Minimal execution example**

Let's create a workflow with the compute modules found in `./Limes-compute-modules/metagenomics`

```python
#!/bin/python3.10
import limes_x as lx

modules = []
modules += lx.LoadComputeModules("./Limes-compute-modules/logistics")
modules = lx.LoadComputeModules("./Limes-compute-modules/metagenomics")
wf = lx.Workflow(
    compute_modules=modules,
    reference_folder="./lx_ref",
)
```

We will then run the workflow (wf) by indicating the desired data products and giving an SRA accession string as the input. A [sequence read archive](https://www.ncbi.nlm.nih.gov/sra) (SRA) accession points to DNA sequnces hosted by the National Center for Biotechnology Information. 
>**NOTE:** While multiple `InputGroups` can be provided, each must have identical formats (same `Items`). This is a bug.

```python
wf.Run(
    workspace="./test_workspace", #Path to the output directory
    targets=[
        lx.Item('metagenomic gzipped reads'),
        lx.Item('metagenomic assembly'),
        lx.Item("metagenomic bin"),
        lx.Item('bin taxonomy table'),
        lx.Item('assembly taxonomy table'),
        lx.Item('genomic annotation'),
    ],
    given=[
        lx.InputGroup(  
            group_by=(lx.Item("sra accession"), "SRR19573024"), 
            children={lx.Item("username"): "user"}, #Switch to include your username 
        )
    ],
    executor=lx.Executor(),
    max_concurrent=1,
)
```
Workspace format:

```
├── ./test_workspace
    ├── comms.json
    ├── comms.lock
    ├── limesx_src.tgz
    ├── input_paths.tsv
    ├── workflow_state.json

    ├── <module name>--######
        ├── context.json
        ├── result.json
        ├── <module ouputs>

    ├── inputs
        ├── <soft links to each input file/folder>
        
    ├── outputs
        ├── <data type (Item)>
            ├── <each instance of Item produced>
```

To run the workflow on multiple SRA accessions use the `pandas` package in python to read in an accession list as show below:

```python
#!/bin/python3.10
import limes_x as lx
import pandas as pd

modules = []
modules += lx.LoadComputeModules("path/to/Limes-compute-modules/logistics") #Change this based on your file path
modules += lx.LoadComputeModules("path/to/Limes-compute-modules/metagenomics") #Change this based on your file path

data = pd.read_csv("accession_list.csv")

wf = lx.Workflow(
    compute_modules=modules,
    reference_folder="path/to/lx_ref", #Change this based on your file path
)

wf.Run(
    workspace="path/to/output/directory", #Change this based on your file path
    targets=[
        lx.Item('metagenomic gzipped reads'),
        lx.Item('metagenomic assembly'),
        lx.Item("metagenomic bin"),
        lx.Item('bin taxonomy table'),
        lx.Item('assembly taxonomy table'),
        lx.Item('genomic annotation'),
    ],
    given=[
        lx.InputGroup(  
            group_by=(lx.Item("sra accession"), row["column_name"]), # column_name corresponds to the column containing the list of accessions
            children={lx.Item("username"): "user"}, # use "whoami" in bash
        )
    for i, row in data.iterrows()],
    executor=lx.Executor(),
    max_concurrent=1,
)
```

# Different execution environments

The default executor will run modules locally. 
```python
wf.Run(
    ...
    executor=lx.Executor(),
)
```

We can also use the `HpcExecutor` to interface with high performance compute clusters (HPC) by specifying how to interact with the cluster's scheduler. Given below is a minimal execution example to run the workflow on cedar with a slurm preset. 

Before running this example make sure you have created a [virtual environment](https://docs.python.org/3/library/venv.html) and installed Limes-x:
```bash
pip install limes-x
git clone https://github.com/hallamlab/Limes-compute-modules.git
python ./Limes-compute-module/setup_modules.py ./lx_ref
```

```python
import sys
from pathlib import Path
import limes_x as lx
from limes_x.presets.slurm import Run

modules = []
modules += lx.LoadComputeModules("path/to/Limes-compute-modules/logistics") #Change this based on your file path
modules += lx.LoadComputeModules("path/to/Limes-compute-modules/metagenomics") #Change this based on your file path

Run(
    modules = modules,
    reference_folder = "path/to/lx_ref",
    workspace="path/to/output/directory", #Change this based on your file path
    targets=[
        lx.Item('metagenomic gzipped reads'),
        lx.Item('metagenomic assembly'),
        lx.Item("metagenomic bin"),
        lx.Item('bin taxonomy table'),
        lx.Item('assembly taxonomy table'),
        lx.Item('genomic annotation'),
    ],
    given=[
        lx.InputGroup(  
            group_by=(lx.Item("sra accession"), "SRR"), 
            children={
              lx.Item("username"): "user"}, # use "whoami" in bash
        )
    ],
    allocation="alloc_code",            # This would be the allocation code on cedar
    time="48:00:00",
    name="Trial run",                  # Change this to the name you want to give your run
)
```

# Making new modules

First, use Limes to generate a template in the folder where you want to keep all of your compute modules.
```python
import limes_x as lx

lx.ModuleBuilder.GenerateTemplate(
    modules_folder = "./compute_modules",
    name = "a descriptive name",
)
```

```
├── ./compute_modules
    ├── <module name>
        ├── lib
            ├── definition.py
        ├── setup
            ├── setup.smk

    ├── <module name>
        ├── lib
        ├── setup
    .
    .
    .
```

The `setup` folder contains the snakemake workflow required to install the module. The `lib` folder must contain (or link to) all scripts required by the compute module. Limes will invoke the module by loading `definition.py` and looking for a `MODULE` variable that holds the compute module.

```python
# template definition.py
from pathlib import Path
from limes_x import ModuleBuilder, Item, JobContext, JobResult

A = Item('a')
B = Item('b')

DEPENDENCY = "image.sif"

def procedure(context: JobContext) -> JobResult:
    input_path = context.manifest[A]
    output_path = context.output_folder.joinpath('copied_file')
    context.shell(f"cp {input_path} {output_path}")
    return JobResult(
        manifest = {
            B: Path(output_path)
        },
    )

MODULE = ModuleBuilder()\
    .SetProcedure(procedure)\
    .AddInput(A, groupby=None)\
    .PromiseOutput(B)\
    .Requires({DEPENDENCY})\
    .SuggestedResources(threads=1, memory_gb=4)\
    .SetHome(__file__, name=None)\
    .Build()
```

[For some examples, take a look at this repo.](https://github.com/hallamlab/Limes-compute-modules)
