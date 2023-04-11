from typing import List
import sys,os,json
from dflow import SlurmRemoteExecutor, Step, Workflow, argo_range,upload_artifact,download_artifact
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, Slices
from pathlib import Path
import dpdata
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import time
from monty.serialization import loadfn

import tcflow,matplotlib,sportran
from tcflow.NEMD_MD_OPs import RunNEMD
from tcflow.NEMD_reprocess_OPs import MakeConfigurations,analysis
from tcflow.input_gen import NVT_input,NVE_input
try:
    import sportran as st
except ImportError:
    from sys import path
    path.append('..')
    import sportran as st

from dflow.plugins import bohrium
from dflow.plugins.bohrium import TiefblueClient
from dflow import config, s3_config
from dflow.plugins.dispatcher import DispatcherExecutor

machine_param = loadfn("machine.json")
email = machine_param.get("email",None)
password = machine_param.get("password",None)
program_id = machine_param.get("program_id",None)
lammps_image = machine_param.get("lammps_image",None)
tc_image = machine_param.get("tc_image",None)
cpu_scass_type = machine_param.get("cpu_scass_type",None)
gpu_scass_type = machine_param.get("gpu_scass_type",None)

config["host"] = "https://workflows.deepmodeling.com"
config["k8s_api_server"] = "https://workflows.deepmodeling.com"
bohrium.config["username"] = email
bohrium.config["password"] = password
bohrium.config["program_id"] = program_id
s3_config["repo_key"] = "oss-bohrium"
s3_config["storage_client"] = TiefblueClient()
upload_python_packages=[tcflow.__path__[0],matplotlib.__path__[0],sportran.__path__[0]]




if __name__ == "__main__":
    # run ../scripts/start-slurm.sh first to start up a slurm cluster
    import input_gen
    with open("parameters.json",'r') as fp :
        param = json.load(fp)
    input_gen.NVT_input(param)
    NVT_input = upload_artifact("NVT.lammps")
    data_input = upload_artifact(param["structure"])
    force_field = upload_artifact(param["force_field"])
    gen = upload_artifact("input_gen.py")

    wf = Workflow("nemd-heat-conductivity")
    SuperCell = Step("SuperCells",
                PythonOPTemplate(MakeSuperCells,image=tc_image),
                parameters={"param":param},
                artifacts={"unit_cell":data_input},
                executor=cpu_scass_type)
    wf.add(SuperCell)
    NEMD = Step("NEMD",
                 PythonOPTemplate(RunNEMD,
                                  slices=Slices("{{item}}",image=lammps_image,python_packages=upload_python_packages,
                                                #sub_path=True,
                                                input_parameter=["name"],
                                                input_artifact=["data"],
                                                output_artifact=["raw"],
                                                )
                                  ),
                 parameters={"name":SuperCell.outputs.parameters["name"],"param":param,},
                 artifacts={"data":SuperCell.outputs.artifacts["supercells"],"input_gen":gen,"force_field":force_field},
                 with_param=argo_range(len(param['supercell'])),
                 key="nemd-{{item}}",
                 executor=gpu_scass_type)
    wf.add(NEMD)
    thermal_conductivity = Step("nemd-extrapolation",
                PythonOPTemplate(analysis,image=tc_image),
                parameters={"param":param},
                artifacts={"dat": NEMD.outputs.artifacts["raw"]},
                executor=cpu_scass_type)
    wf.add(thermal_conductivity)
    wf.submit()
