from typing import List
import os,json
from dflow import SlurmRemoteExecutor, Step, Workflow, argo_range,upload_artifact,download_artifact
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, Slices
from pathlib import Path
import dpdata
from . import input_gen


class RunNEMD(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "name":str,
            "param":dict,
            "data":Artifact(Path),
            #"input_gen":Artifact(Path),
            "force_field":Artifact(List[Path]),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "raw":Artifact(Path),
            # "data": Artifact(Path),
            # "log": Artifact(Path),
            # "temp_profile":Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        name = op_in["name"]
        param = op_in["param"]
        data = op_in["data"]
        #gen = op_in["input_gen"]
        force_field = op_in["force_field"]
        #Path('input_gen.py').symlink_to(gen)
        #import input_gen
        name=Path(name)
        name.mkdir()
        cwd = os.getcwd()
        os.chdir(name)
        for field in force_field:
            Path(field.parts[-1]).symlink_to(field)
        Path('data.lammps').symlink_to(data)
        input_gen.NEMD_input(param)
        os.system(f"{param["md_command"]}")
        # os.system(f"mv log.lammps {name}.log")
        # logfile=Path(f"{name}.log")
        os.chdir(cwd)
        allfile= name
        # logfile=name/"log.lammps"
        # datafile=name/"data.lammps"
        # tempfile=name/"temp_profile.txt"
        op_out = OPIO({
            "raw":allfile,
            # "data":datafile,
            # "log":logfile,
            # "temp_profile":tempfile,
        })
        return op_out
