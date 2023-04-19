<img src="https://raw.githubusercontent.com/Tony-xy-Liu/Limes-x/main/docs/images/Limes-x_logo.svg" alt="Limes-x"/>

### *Workflows on demand!*

# For the impatient

### **Dependencies**
- Python
- Singularity
- Git (to setup existing modules)
- Snakemake (to setup existing modules)

### **Setup**

```bash
#!/bin/bash
pip install limes-x

git clone https://github.com/hallam_lab/Limes-compute-modules.git
python ./Limes-compute-module/setup_modules.py ./lx_ref
```

### **Run**

```python
#!/bin/python3.10
import limes_x as lx

modules = lx.LoadComputeModules("./Limes-compute-modules/metagenomics")
wf = lx.Workflow(
    compute_modules=modules,
    reference_folder="./lx_ref",
)

wf.Run(
    workspace="./test_workspace",
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
            children={Item("username"): "Steven"}, # use "whoami" in bash
        )
    ],
    executor=lx.Executor(),
)
```

# Dependencies

- Anaconda (optional, but recommended)
    - [faster version (Mamba)](https://mamba.readthedocs.io/en/latest/installation.html); Use "mamba" instead of "conda" below
    - [plain Anaconda]()
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

Limes-x finds the set of compute modules required to convert the given inputs to the desired inputs. This set of modules is then joined together into an execution-ready workflow. 

[A list of available compute modules can be found at this repo](https://github.com/Tony-xy-Liu/Limes-compute-modules)
<br>
Use the `setup_modules` script to install each module's dependencies and reference databases using Singularity and Snakemake. 
```bash
git clone https://github.com/hallam_lab/Limes-compute-modules.git
python ./Limes-compute-module/setup_modules.py ./lx_ref
```

### **Minimal execution example**

Create a workflow with the compute modules found in `./Limes-compute-modules/metagenomics`

```python
#!/bin/python3.10
import limes_x as lx

modules = lx.LoadComputeModules("./Limes-compute-modules/metagenomics")
wf = lx.Workflow(
    compute_modules=modules,
    reference_folder="./lx_ref",
)
```

Run the workflow by indicating the desired data products and giving an SRA accession string as the input. A [sequence read archive](https://www.ncbi.nlm.nih.gov/sra) (SRA) accession points to DNA sequnces hosted by the National Center for Biotechnology Information. 
>**NOTE:** While multiple InputGroups can be provided, each must have identical formats (same Items). This is a bug.

```python
wf.Run(
    workspace="./test_workspace",
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
            children={Item("username"): "Steven"},
        )
    ],
    executor=lx.Executor(),
)
```
Expected output format:

```
├── ./test_workspace
    ├── comms.json
    ├── comms.lock
    ├── limesx_src.tgz
    ├── input_paths.tsv
    ├── workflow_state.json

    ├── <module name>--<######>     
        ├── context.json
        ├── result.json
        ├── <module ouputs>

    ├── inputs
        ├── <soft links to each input file/folder>
        
    ├── outputs
        ├── <data type (Item)>
            ├── <each instance of Item produced>
```

# Different execution environments

# Making new modules
