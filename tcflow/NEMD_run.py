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
from tcflow.NEMD_reprocess_OPs import MakeSuperCells,analysis
from tcflow import input_gen
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

def load_machine(machine_param):
    type=machine_param.get("machine_type",None)
    if type=="bohrium":
        email = machine_param.get("email",None)
        password = machine_param.get("password",None)
        program_id = machine_param.get("program_id",None)
        lammps_image = machine_param.get("lammps_image",None)
        tc_image = machine_param.get("tc_image",None)
        cpu_scass_type = machine_param.get("cpu_scass_type",None)
        gpu_scass_type = machine_param.get("gpu_scass_type",None)
        gpu_dispatcher_executor = DispatcherExecutor(
            machine_dict={
                "batch_type": "Bohrium",
                "context_type": "Bohrium",
                "remote_profile": {
                    "email": email,
                    "password": password,
                    "program_id": program_id,
                    "input_data": {
                        "job_type": "container",
                        "platform": "ali",
                        "scass_type": gpu_scass_type,
                    },
                },
            },
        )
        cpu_dispatcher_executor = DispatcherExecutor(
            machine_dict={
                "batch_type": "Bohrium",
                "context_type": "Bohrium",
                "remote_profile": {
                    "email": email,
                    "password": password,
                    "program_id": program_id,
                    "input_data": {
                        "job_type": "container",
                        "platform": "ali",
                        "scass_type": cpu_scass_type,
                    },
                },
            },
        )
        return gpu_dispatcher_executor,cpu_dispatcher_executor
    elif type=="cluster":
        host=machine_param.get("host",None)
        port=machine_param.get("port",None)
        username=machine_param.get("username",None)
        password=machine_param.get("ssh_password",None)
        remote_root=machine_param.get("remote_root",None)
        md_machine_dict=machine_param.get("md_machine_dict",None)
        md_resources_dict=machine_param.get("md_resources_dict",None)
        analysis_machine_dict=machine_param.get("analysis_machine_dict",None)
        analysis_resources_dict=machine_param.get("analysis_resources_dict",None)
        cpu_dispatcher_executor = DispatcherExecutor(
            host=host,
            port=port,
            username=username,
            password=password,
            remote_root=remote_root,
            image_pull_policy="IfNotPresent",
            machine_dict=analysis_machine_dict,
            resources_dict=analysis_resources_dict,
            merge_sliced_step=False
        )
        gpu_dispatcher_executor = DispatcherExecutor(
            host=host,
            port=port,
            username=username,
            password=password,
            remote_root=remote_root,
            image_pull_policy="IfNotPresent",
            machine_dict=md_machine_dict,
            resources_dict=md_resources_dict,
            merge_sliced_step=False
        )
        return gpu_dispatcher_executor,cpu_dispatcher_executor
    else:raise ValueError("Unsupported machine_type!")


def NEMD_run_main():
    # run ../scripts/start-slurm.sh first to start up a slurm cluster
    machine_param = loadfn("machine.json")
    gpu_dispatcher_executor,cpu_dispatcher_executor=load_machine(machine_param)
    upload_python_packages=[tcflow.__path__[0],matplotlib.__path__[0],sportran.__path__[0]]
    param = loadfn("parameters.json")
    type=machine_param.get("machine_type",None)
    lammps_image = machine_param.get("lammps_image",None)
    tc_image = machine_param.get("tc_image",None)
    input_gen.NVT_input(param)
    NVT_input = upload_artifact("NVT.lammps")
    data_input = upload_artifact(param["structure"])
    if (param["force_field"]):force_field = upload_artifact(param["force_field"])
    else:force_field = upload_artifact(param["structure"])


    wf = Workflow("nemd-tc")
    SuperCell = Step("SuperCells",
                PythonOPTemplate(MakeSuperCells,image="python:3.8" if type=="cluster" else tc_image),
                parameters={"param":param},
                artifacts={"unit_cell":data_input},
                executor=cpu_dispatcher_executor)
    wf.add(SuperCell)
    NEMD = Step("NEMD",
                 PythonOPTemplate(RunNEMD,image="python:3.8" if type=="cluster" else lammps_image,python_packages=upload_python_packages,
                                  slices=Slices("{{item}}",
                                                #sub_path=True,
                                                input_parameter=["name"],
                                                input_artifact=["data"],
                                                output_artifact=["raw"],
                                                )
                                  ),
                 parameters={"name":SuperCell.outputs.parameters["name"],"param":param,},
                 artifacts={"data":SuperCell.outputs.artifacts["supercells"],"force_field":force_field},#"input_gen":gen,
                 with_param=argo_range(len(param['supercell'])),
                 key="nemd-{{item}}",
                 executor=gpu_dispatcher_executor)
    wf.add(NEMD)
    thermal_conductivity = Step("nemd-extrapolation",
                PythonOPTemplate(analysis,image="python:3.8" if type=="cluster" else tc_image),
                parameters={"param":param},
                artifacts={"dat": NEMD.outputs.artifacts["raw"]},
                executor=cpu_dispatcher_executor)
    wf.add(thermal_conductivity)
    wf.submit()
