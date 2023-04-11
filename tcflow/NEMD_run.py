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

class MakeSuperCells(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "unit_cell":Artifact(Path),
            "param":dict
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "name":List[str],
            "supercells":Artifact(List[Path]),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        unit_cell =op_in["unit_cell"]
        cell_size = op_in["param"]["supercell"]
        oname=[]
        osupercells=[]
        data = dpdata.System(unit_cell,"lammps/lmp")
        for size in cell_size:
            ofolder = Path(f"task.{size[0]}x{size[1]}x{size[2]}")
            oname.append(str(ofolder))
            ofolder.mkdir()
            ofile= ofolder/"data.lammps"
            data.replicate(size).to_lammps_lmp(ofile)
            osupercells.append(ofile)
        op_out = OPIO({
            "name":oname,
            "supercells":osupercells,
        })
        return op_out



def read_file(filename):
    with open(filename, 'r') as f:
        text = f.readlines()
        text = text[3:]
    #pattern=r'\d+\s\d+\s\d+\n'
    blocks = int(text[0].split()[1])
    data = np.zeros(blocks)
    for i in range(blocks): #calculate the average temperature of one block over time
        raw_data = np.array([float(line.split()[-1]) for line in text[i+1::blocks+1]])
        data[i] = np.mean(raw_data)
    return data

def data_process(data, L,name, xmin, xmax):
    position = np.arange(0.5, len(data) + 0.5) * L / len(data)
    position_L=position[int(xmin*len(data)/2):int(xmax*len(data)/2)]
    data_L=data[int(xmin*len(data)/2):int(xmax*len(data)/2)]
    position_R=position[int((xmin+1)*len(data)/2):int((xmax+1)*len(data)/2)]
    data_R=data[int((xmin+1)*len(data)/2):int((xmax+1)*len(data)/2)]
    model_L=stats.linregress(position_L, data_L)
    model_R=stats.linregress(position_R, data_R)
    plt.xlabel(r'Distance($\AA$)')
    plt.ylabel('Temperature($K$)')
    plt.plot(position, data,'ro-')
    plt.title(f"Temperature Distribution {name}")
    plt.savefig(f"temp_{name}.png")
    plt.close()
    return model_L, model_R

def calculate_thermal_conductivity(path,param):
    timestep = param["time_step"] #units ps
    NEMD_production_steps = param["NEMD_production_steps"]
    lattice = path.split('/')[-1].split(".")[-1]
    data = read_file(path+"/temp_profile.txt")
    structure = dpdata.System(path+"/data.lammps","lammps/lmp")
    lx = structure["cells"][0][0][0]
    gradient_L,gradient_R = data_process(data,lx,lattice,param["linear_scale"][0], param["linear_scale"][1])
    log = st.i_o.LAMMPSLogFile(path+'/log.lammps', run_keyword='NEMD_RUN')
    log.read_datalines(start_step=0, NSTEPS=0, select_ckeys=['E_hot','E_cold'])
    Energy = (abs(log.data["E_cold"][-1][0])+abs(log.data["E_hot"][-1][0]))/2 #unit eV
    Energy =  Energy*1.6e-19 #eV -> J
    area = np.linalg.norm(np.cross(structure["cells"][0][1][1:3],structure["cells"][0][2][1:3]))
    dQ=Energy/(timestep*NEMD_production_steps)/area/2 #J/(ps*A^2)
    dQ = dQ/1e-12/(1e-10)**2#J/(ps*A^2) -> W/m^2
    g_L = abs(gradient_L.slope)/1e-10 #K/A->K/m
    thermal_conductivity_L = dQ/g_L
    g_R = abs(gradient_L.slope)/1e-10 #K/A->K/m
    thermal_conductivity_R = dQ/g_R
    thermal_conductivity = (thermal_conductivity_L+thermal_conductivity_R)/2
    l = int(lattice.split("x")[0])
    return l,thermal_conductivity


class analysis(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "param":dict,
            "dat": Artifact(List[str]),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "output":Artifact(Path),
            "plot":Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        dat = op_in["dat"]
        param = op_in["param"]
        print(dat)
        L=[] # length of lattice along x
        kappa =[] # thermal_conductivity
        for d in dat:
            l,k = calculate_thermal_conductivity(d,param)
            L.append(l)
            kappa.append(k)
        L = np.array(L)
        kappa = np.array(kappa)
        extrapolation = stats.linregress(1/L, 1/kappa)
        plt.plot(1/L,1/kappa,'o')
        plt.plot(1/L, extrapolation.intercept + extrapolation.slope*1/L, 'r')
        extra = plt.Rectangle((0, 0), 0, 0, fc="w", fill=False, edgecolor='none', linewidth=0)
        plt.legend([extra],[f"EXTRAP. 1/K = {extrapolation.intercept:04f}+{extrapolation.slope:04f}*1/L" ],loc="upper left",fontsize="small")
        plt.xlabel('1/L($\AA$^-1)')
        plt.ylabel('1/kappa(mK/W)')
        plt.title("Thermal Conductivity:NEMD Extrapolation")
        plt.savefig("thermal_conductivity.png")
        plt.close()
        with open("result.txt",'w') as f:
            for i in range(len(kappa)):
                f.writelines(f"{L[i]}: {kappa[i]}\n")
            f.writelines(f"Thermal Conductivity : {1/extrapolation.intercept:04f} W/mk \n")

        result = Path("result.txt")
        plot = Path("thermal_conductivity.png")
        op_out = OPIO({
            "output":result,
            "plot":plot
        })
        return op_out



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
    slurm_remote_executor = SlurmRemoteExecutor(
        host="ssh.cn-nanjing-n26.paracloud.com",
        port=22,
        username="scv6951@BSCC-N26",
        password="8yn0SwKkogU3Ts92MFOV1h4L6lr57vqp",
        header="""#!/bin/bash
#SBATCH --job-name="lmp_dp"
#SBATCH --error=./err.txt
#SBATCH --output=./stdout.inf
#SBATCH --gpus=1
#SBATCH --time=1000:00:00
source activate deepmd_2.1.4
        """,
        workdir="/data/home/scv6951/zwj/test/{{workflow.name}}/{{pod.name}}",
    )

    wf = Workflow("nemd-heat-conductivity")
    SuperCell = Step("SuperCells",
                PythonOPTemplate(MakeSuperCells,image="python:3.8"),
                parameters={"param":param},
                artifacts={"unit_cell":data_input},
                executor=slurm_remote_executor)
    wf.add(SuperCell)
    NEMD = Step("NEMD",
                 PythonOPTemplate(RunNEMD,
                                  slices=Slices("{{item}}",
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
                 executor=slurm_remote_executor)
    wf.add(NEMD)
    thermal_conductivity = Step("nemd-extrapolation",
                PythonOPTemplate(analysis,image="python:3.8"),
                parameters={"param":param},
                artifacts={"dat": NEMD.outputs.artifacts["raw"]},
                executor=slurm_remote_executor)
    wf.add(thermal_conductivity)
    wf.submit()
