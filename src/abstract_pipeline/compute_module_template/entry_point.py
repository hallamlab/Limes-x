from __entry_setup__ import ME, MODULE_PATH, OUTPUT_DIR, INPUTS, OUTPUT_TEMPLATES, RegisterOutput

import os
from abstract_pipeline.compute_modules import ComputeModule
from data_pointer import ManifestTemplate, Manifest

raise NotImplementedError(f'module [{ME.name}] was created at [{MODULE_PATH}] but never implemented') # delete me!

# get input file paths
path1 = INPUTS['template_1']['field_1a']
path2 = INPUTS['template_1']['field_1b']
path3 = INPUTS['template_2']['field_2a']
# ...


# run the software you need to generate the output files in OUTPUT_DIR
os.system(f'command {path1} {path2} {...} {OUTPUT_DIR}')
# ...


# register the output files
# not recommended: absolute file paths can be used as well, but then intermediate files will not be in the workspace
output_template = OUTPUT_TEMPLATES['template_3']
RegisterOutput(output_template.MakeManifest([
    'path/to/output_file_1',
    'path/to/output_file_2'
]))
