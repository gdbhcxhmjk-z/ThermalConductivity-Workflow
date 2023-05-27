from typing import List
import os,json
from dflow import SlurmRemoteExecutor, Step, Workflow, argo_range,upload_artifact,download_artifact
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, Slices
from pathlib import Path
import dpdata
from . import input_gen

class RunNVT(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "data":Artifact(Path),
            "param":dict,
            "input": Artifact(Path),
            #"input_gen":Artifact(Path),
            "force_field":Artifact(List[Path]),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "dump":Artifact(Path),
            "log": Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        input =op_in["input"]
        data = op_in["data"]
        force_field = op_in["force_field"]
        print(input)
        print(data)
        print(force_field)
        Path('in.lammps').symlink_to(input)
        Path('data.lammps').symlink_to(data)
        for field in force_field:
            Path(field.parts[-1]).symlink_to(field)
        input_gen.NVT_input(param)
        os.system(f"{param["md_command"]}")
        logfile=Path("log.lammps")
        dumpfile=Path("NVT.lammpstrj")
        op_out = OPIO({
            "dump":dumpfile,
            "log":logfile,
        })
        return op_out

class RunNVE(OP):
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
            "dat": Artifact(Path),

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
        input_gen.NVE_input(param)
        os.system(f"{param["md_command"]}")
        os.chdir(cwd)
        logfile=name/"log.lammps"
        op_out = OPIO({
            "dat":logfile,
        })
        return op_out
