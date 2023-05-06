import os, sys
import uuid
from pathlib import Path
import time
import random
from datetime import datetime
from typing import Any, Iterable
import json

#########################################################################################
# This code is horrible, but it was just meant
# to be a "quick" and dirty convenience script

class Parser:
    def __init__(self, data: Any) -> None:
        self.data = data
    
    def ToDict(self):
        return dict(value=str(self.data), type=str(type(self.data)))
    
    @classmethod
    def FromDict(cls, data:dict):
        val = data["value"]
        return {
            "<class 'pathlib.PosixPath'>": lambda: Path(val),
        }.get(data["type"], lambda: data["value"])()

SCRIPT = os.path.abspath(__file__)
SCRIPT_DIR = Path("/".join(SCRIPT.split("/")[:-1]))
_INNER = "inner"
_CONTEXT_FILE = "slurm_context.json"
if (len(sys.argv)>1 and sys.argv[1] == _INNER):
    #------------------------------------------------------------------------------------------
    # load context and setup

    run_folder = Path(sys.argv[2])
    os.chdir(run_folder)
    WS = Path("./")
    with open(_CONTEXT_FILE, "r") as f:
        CONTEXT: dict[str, Any] = json.load(f)
    sys.path = CONTEXT["pythonpath"]
    import limes_x as lx
    from limes_x import Item, InputGroup

    def _parse_given(g: dict):
        root_k = Item(g["root_k"])
        root_v = Parser.FromDict(g["root_v"])

        children = {}
        for k, v in g.get("children", {}).items():
            if isinstance(v, list):
                v = [Parser.FromDict(x) for x in v]
            else:
                v = Parser.FromDict(v)
            children[Item(k)] = v

        return InputGroup(
            group_by=(root_k, root_v),
            children=children,
        )
    
    run_id: str = CONTEXT["run_id"]
    allocation: str = CONTEXT["allocation"]
    module_locations = CONTEXT["modules"]
    modules = [lx.ComputeModule._load(p) for p in module_locations]
    reference_folder: Path = CONTEXT["reference_folder"]
    given: Iterable[lx.InputGroup] = [_parse_given(g) for g in CONTEXT["given"]]
    targets: Iterable[lx.Item] = [Item(t) for t in CONTEXT["targets"]]

    wf = lx.Workflow(modules, reference_folder)

    #------------------------------------------------------------------------------------------
    # slurm executor preset

    def get_res(job: str, manifest: dict, cores, mem):
        def _tax_bin():
            bins = manifest.get(Item('metagenomic bin'), [])
            if not isinstance(bins, list): bins = [bins]
            hrs = (0.33*len(bins)) + 2 # 20mins/bin + 2hr grace
            hrs = min(hrs, 48)
            return 4, hrs, 60 # max mem for 2 jobs/most common node

        def _asm():
            rpaths = manifest.get(Item('metagenomic gzipped reads'), [])
            if not isinstance(rpaths, list): rpaths = [rpaths]
            rstats = [os.stat(p) for p in rpaths]
            rsize = sum([s.st_size / (1024**3) for s in rstats]) # gb

            if rsize > 5:
                return 24, 36, 100
            elif rsize > 0.6:
                return 16, 24, 60
            else:
                return cores, 12, mem

        _cores, _time, _mem = {
            "download_sra":             lambda: (cores, 4,  mem),
            "extract_mg-reads":         lambda: (cores, 4,  mem),
            "metagenomic_assembly":     _asm,
            "metagenomic_binning":      lambda: (16, 24, 60), # todo scale this
            "taxonomy_on_bin":          _tax_bin,
            "taxonomy_on_assembly":     lambda: (2, 1,  48),
            "checkm_on_bin":            lambda: (cores, 1,  mem),
            "annotation_metapathways":  lambda: (cores, 16,  mem),
        }.get(job, lambda: (cores, 4, mem))()

        _hrs = int(_time)
        _mins = int(round(_time-_hrs)*60)
        _time_str = f"{_hrs:02}:{_mins:02}:00"
        return (_cores, _time_str, _mem)

    def slurm(job: lx.Job) -> tuple[bool, str]:
        p = job.context.params
        time.sleep(10*random.random())
        cores, time_str, mem = get_res(job.instance.step.name, job.context.manifest, p.threads, p.mem_gb)
        p.threads = cores
        p.mem_gb = mem
        job.SaveContext()
        return job.Shell(f"""\
            sbatch --wait --account={allocation} \
                --job-name="{run_id}-{job.instance.step.name}:{job.instance.GetID()}" \
                --nodes=1 --ntasks=1 \
                --cpus-per-task={cores} --mem={mem}G --time={time_str} \
                --wrap="{job.run_command}"\
        """)
    
    #------------------------------------------------------------------------------------------
    # run workflow

    now = datetime.now() 
    start_date = now.strftime("%Y-%m-%d")
    ex = lx.HpcExecutor(hpc_procedure=slurm, tmp_dir_name="SLURM_TMPDIR")
    ex.max_active_io_jobs = 128
    wf.Run(
        workspace=WS,
        targets=targets,
        given=given,
        executor=ex,
        max_per_module={"download_sra": 10},
    )

    #------------------------------------------------------------------------------------------
    # cleanup and save important info

    slurm_ids = set()
    for f in os.listdir(WS):
        if "slurm-" not in f: continue
        slurm_ids.add(f.replace("slurm-", "").replace(".out", ""))

    # todo: get the info as jobs finish
    slurm_temp = Path(WS).joinpath("slurm_temp")
    os.system(f"sacct -o jobid%20,jobname%50,systemcpu,usercpu,maxrss,alloccpus,reqmem,elapsed%12,state%20 -S {start_date}>{slurm_temp}")
    log_entries = []
    with open(slurm_temp) as f:
        # 2 header lines
        log_entries.append(f.readline())
        log_entries.append(f.readline())
        for l in f:
            id = l.strip().split(" ")[0]
            id = id.split(".")[0]
            if id not in slurm_ids: continue
            log_entries.append(l)

    with open(Path(WS).joinpath("slurm_stats.txt"), "w") as f:
        f.writelines(log_entries)

    for id in slurm_ids:
        os.system(f"""rm {WS.joinpath(f"/slurm-{id}.out")}""")
    os.system(f"rm {slurm_temp}")

#########################################################################################
# The starting point is down here
# because this script is submitted to slurm
# and when it first starts, all the python dependencies
# are gone and have to be loaded from the context file

from limes_x import InputGroup, ComputeModule, Item

def Run(
    modules: list[ComputeModule],
    reference_folder: str|Path,
    workspace: str|Path,
    targets: Iterable[str|Item],
    given: Iterable[InputGroup],
    allocation: str,
    time: str="48:00:00",
    name: str|None=None,
    continue_from: str|None=None,
):
    def _ok_for_path(c: str):
        return c.isalpha() or c.isdigit() or c in "-_"
    if name is not None: name = "".join([c if _ok_for_path(c) else "_" for c in name])

    workspace = Path(os.path.abspath(workspace))
    if continue_from is None:
        now = datetime.now() 
        date_time = now.strftime("%Y-%m-%d_%H-%M")
        run_id = f'{uuid.uuid4().hex[:3]}'
        run_name = f"{run_id}-{name}" if name is not None else f"{run_id}-lx"
        run_folder = workspace.joinpath(f"{date_time}.{run_name}"); os.makedirs(run_folder)
    else:
        run_folder = workspace.joinpath(continue_from)
        assert run_folder.exists(), f"path {run_folder} doesn't exist"
        run_name = continue_from.split(".")[1]
        run_id = run_name.split("-")[0]
        date_time = continue_from.split(".")[0]

    def _parse_given(g: InputGroup):
        children = {}
        for k, v in g.children.items():
            if isinstance(v, list):
                if len(v) > 1:
                    v = [Parser(x).ToDict() for x in v]
                else:
                    v = Parser(v[0]).ToDict()
            else:
                v = Parser(v).ToDict()
            children[k.key] = v
        return dict(
            root_k = g.root_type.key, 
            root_v = Parser(g.root_value).ToDict(),
            children = children,
        )

    reference_folder = os.path.abspath(reference_folder)
    with open(run_folder.joinpath(_CONTEXT_FILE), 'w') as f:
        context = dict(
            pythonpath = [os.path.abspath(p) for p in sys.path],
            run_id = run_id,
            allocation = allocation,
            modules = [str(m.location) for m in modules],
            reference_folder = reference_folder,
            given = [_parse_given(g) for g in given],
            targets = [t if isinstance(t, str) else t.key for t in  targets],
        )
        json.dump(context, f, indent=4)

    os.chdir(run_folder)
    out_log = "slurm.cmd"
    cmd = f"""\
    sbatch --account={allocation} \
        --job-name="{run_name}" \
        --nodes=1 --ntasks=1 --error slurm.err --output slurm.out \
        --cpus-per-task=1 --mem=4G --time={time} \
        --wrap="python {SCRIPT} {_INNER} {run_folder}" >> {out_log}
    """.replace("  ", "")
    with open(out_log, "w") as log:
        log.write(cmd+"\n")
    os.system(cmd)
    with open(out_log, "r+") as log:
        for l in log:
            print(l, end="")
