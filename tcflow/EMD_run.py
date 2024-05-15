from typing import List
import sys,os,json
from dflow import SlurmRemoteExecutor, Step, Workflow, argo_range,upload_artifact,download_artifact
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, Slices
from pathlib import Path
import dpdata
import numpy as np
import time
from monty.serialization import loadfn

import tcflow,matplotlib,sportran
from tcflow.EMD_MD_OPs import RunNVT,RunNVE
from tcflow.EMD_reprocess_OPs import MakeConfigurations,analysis
from tcflow import input_gen

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


def EMD_run_main():
    # run ../scripts/start-slurm.sh first to start up a slurm cluster
    # from EMD_OPs import RunNVT,RunNVE,MakeConfigurations,analysis
    machine_param = loadfn("machine.json")
    email = machine_param.get("email",None)
    password = machine_param.get("password",None)
    program_id = machine_param.get("program_id",None)
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
    gpu_dispatcher_executor,cpu_dispatcher_executor=load_machine(machine_param)

    wf = Workflow("emd-tc")
    NVT = Step("NVT",
                PythonOPTemplate(RunNVT,image="python:3.8" if type=="cluster" else lammps_image,python_packages=upload_python_packages,),
                parameters={"param":param},
                artifacts={"data":data_input,"input":NVT_input,"force_field":force_field},#"input_gen":gen,
                executor=gpu_dispatcher_executor)
    wf.add(NVT)
    Configurations=Step("Config",
                PythonOPTemplate(MakeConfigurations,image="python:3.8" if type=="cluster" else tc_image),
                parameters={"numb_lmp":param['num_configurations']},
                artifacts={"dump": NVT.outputs.artifacts["dump"]},
                executor=cpu_dispatcher_executor)
    wf.add(Configurations)
    NVE = Step("NVE",
                 PythonOPTemplate(RunNVE,image="python:3.8" if type=="cluster" else lammps_image ,python_packages=upload_python_packages,
                                  slices=Slices("{{item}}",
                                                #sub_path=True,
                                                input_parameter=["name"],
                                                input_artifact=["data"],
                                                output_artifact=["dat"],
                                                )
                                  ),
                 parameters={"name":Configurations.outputs.parameters["name"],"param":param,},
                 artifacts={"data":Configurations.outputs.artifacts["configurations"],"force_field":force_field},#"input_gen":gen,
                 with_param=argo_range(param['num_configurations']),
                 key="nve-{{item}}",
                 executor=gpu_dispatcher_executor)
    wf.add(NVE)
    thermal_conductivity = Step("cepstral-analysis",
                PythonOPTemplate(analysis,image="python:3.8" if type=="cluster" else tc_image,),
                parameters={"param":param},
                artifacts={"dat": NVE.outputs.artifacts["dat"]},
                executor=cpu_dispatcher_executor)
    wf.add(thermal_conductivity)
    wf.submit()
