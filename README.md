<img src="https://raw.githubusercontent.com/hallamlab/Limes-x/main/docs/images/Limes-x_logo.svg" alt="Limes-x"/>

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

git clone https://github.com/hallamlab/Limes-compute-modules.git
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
git clone https://github.com/hallamlab/Limes-compute-modules.git
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
>**NOTE:** While multiple `InputGroups` can be provided, each must have identical formats (same `Items`). This is a bug.

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

# Different execution environments

The default executor will run modules locally. 
```python
wf.Run(
    ...
    executor=lx.Executor(),
)
```

We can use the `HpcExecutor` to interface with high performance compute clusters (HPC) by specifying how to interact with the cluster's scheduler. Here, we write the callback function, `schedule_job`, which will be called when a compute module needs to be executed on the cluster. The executor will pass in a `job` object to our function that provides a `shell`, the `run_command` to execute the compute module.

```python
from limes_x import Job

def schedule_job(job: Job) -> tuple[bool, str]:
    return job.Shell(f"""\
        <schedule a job with the following command>
        {job.run_command}
    """)

ex = lx.HpcExecutor(
    hpc_procedure=schedule_job,
    tmp_dir_name="TMP"
)
wf.Run(
    ...
    executor=ex,
)
```

`tmp_dir_name` is the environment variable that stores the path to the temporary directory on the worker node. The `HpcExecutor` will transfer all required files/folders there before running the job.

Below is an example with `slurm`, the scheduler used by the Digital Alliance of Canada's Cedar cluster.

```python
def get_res(job: str, manifest: dict, cores, mem):
    _cores, _hrs, _mem = {
        "download_sra":             lambda: (cores, 4,  mem),
        "extract_mg-reads":         lambda: (cores, 4,  mem),
        "metagenomic_assembly":     lambda: (cores, 12, mem),
        "metagenomic_binning":      lambda: (cores, 24, mem),
        "taxonomy_bin":             lambda: (cores, 4,  mem),
        "taxonomy_assembly":        lambda: (cores, 4,  mem),
        "checkm_on_bin":            lambda: (cores, 1,  mem),
        "annotation_metapathways":  lambda: (cores, 8,  mem),
    }.get(job, lambda: (cores, 4, mem))()
    return (_cores, _hrs, _mem)

def slurm(job: lx.Job) -> tuple[bool, str]:
    p = job.context.params
    time.sleep(2*random.random())
    job_name = job.instance.step.name
    job_id = job.instance.GetID()
    cores, hrs, mem = get_res(
        job_name,
        job.context.manifest,
        p.threads,
        p.mem_gb
    )
    return job.Shell(f"""\
        sbatch --wait --account={ALLOC} \
            --job-name="lx-{job_name}:{job_id}" \
            --nodes=1 --ntasks=1 \
            --cpus-per-task={cores} --mem={mem}G --time={hrs}:00:00 \
            --wrap="{job.run_command}"\
    """)

ex = lx.HpcExecutor(
    hpc_procedure=slurm,
    tmp_dir_name="SLURM_TMPDIR"
)
wf.Run(
    ...
    executor=ex,
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
