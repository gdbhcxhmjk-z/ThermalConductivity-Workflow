from typing import List
import sys,os,json
from dflow import SlurmRemoteExecutor, Step, Workflow, argo_range,upload_artifact,download_artifact
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, Slices
from pathlib import Path
import dpdata
import numpy as np
import time
from monty.serialization import loadfn

import dflowTC
from dflowTC.EMD_OPs import RunNVT,RunNVE,MakeConfigurations,analysis
from dflowTC.input_gen import NVT_input,NVE_input

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




if __name__ == "__main__":
    # run ../scripts/start-slurm.sh first to start up a slurm cluster
    # from EMD_OPs import RunNVT,RunNVE,MakeConfigurations,analysis
    param = loadfn("parameters.json")
    NVT_input(param)
    NVT_input = upload_artifact("NVT.lammps")
    data_input = upload_artifact(param["structure"])
    force_field = upload_artifact(param["force_field"])
    extract_input = upload_artifact("extract_lammps_thermo.sh")
    gen = upload_artifact("input_gen.py")
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

    wf = Workflow("emd-tc")
    NVT = Step("NVT",
                PythonOPTemplate(RunNVT,image=lammps_image),
                artifacts={"data":data_input,"input":NVT_input,"input_gen":gen,"force_field":force_field},
                executor=gpu_dispatcher_executor)
    wf.add(NVT)
    Configurations=Step("Config",
                PythonOPTemplate(MakeConfigurations,image=tc_image,upload_python_packages=[dflowTC.__path__[0]]),
                parameters={"numb_lmp":param['num_configurations']},
                artifacts={"dump": NVT.outputs.artifacts["dump"]},
                executor=cpu_dispatcher_executor)
    wf.add(Configurations)
    NVE = Step("NVE",
                 PythonOPTemplate(RunNVE,image=lammps_image,
                                  slices=Slices("{{item}}",
                                                #sub_path=True,
                                                input_parameter=["name"],
                                                input_artifact=["data"],
                                                output_artifact=["dat","log"],
                                                )
                                  ),
                 parameters={"name":Configurations.outputs.parameters["name"],"param":param,},
                 artifacts={"data":Configurations.outputs.artifacts["configurations"],"extract_sh":extract_input,"input_gen":gen,"force_field":force_field},
                 with_param=argo_range(param['num_configurations']),
                 key="nve-{{item}}",
                 executor=gpu_dispatcher_executor)
    wf.add(NVE)
    thermal_conductivity = Step("cepstral-analysis",
                PythonOPTemplate(analysis,image=tc_image,upload_python_packages=[dflowTC.__path__[0]]),
                parameters={"param":param},
                artifacts={"dat": NVE.outputs.artifacts["dat"]},
                executor=cpu_dispatcher_executor)
    wf.add(thermal_conductivity)
    # check = Step("check",
    #              PythonOPTemplate(Check, image="python:3.8"),
    #              artifacts={"foo": hello.outputs.artifacts["foo"]})
    # wf.add(check)
    wf.submit()
