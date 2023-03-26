from typing import List
import os,json
from dflow import SlurmRemoteExecutor, Step, Workflow, argo_range,upload_artifact,download_artifact
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, Slices
from pathlib import Path
import dpdata
import numpy as np
import matplotlib.pyplot as plt
try:
    import sportran as st
except ImportError:
    from sys import path
    path.append('..')
    import sportran as st

class RunNVT(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "data":Artifact(Path),
            "input": Artifact(Path),
            "input_gen":Artifact(Path),
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
        os.system(f"mpirun -n 1 lmp < in.lammps")
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
            "input_gen":Artifact(Path),
            "extract_sh": Artifact(Path),
            "force_field":Artifact(List[Path]),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "dat": Artifact(Path),
            "log": Artifact(Path),

        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        name = op_in["name"]
        param = op_in["param"]
        data = op_in["data"]
        gen = op_in["input_gen"]
        extract_sh = op_in["extract_sh"]
        force_field = op_in["force_field"]
        Path('input_gen.py').symlink_to(gen)
        import input_gen
        name=Path(name)
        name.mkdir()
        cwd = os.getcwd()
        os.chdir(name)
        for field in force_field:
            Path(field.parts[-1]).symlink_to(field)
        Path('data.lammps').symlink_to(data)
        Path('extract_lammps_thermo.sh').symlink_to(extract_sh)
        input_gen.NVE_input(param)
        os.system(f"mpirun -n 1 lmp < in.lammps")
        # os.system(f"mv log.lammps {name}.log")
        # logfile=Path(f"{name}.log")
        os.system(f"bash extract_lammps_thermo.sh -f log.lammps -d 'DUMP_RUN' > sportran.dat")
        os.chdir(cwd)
        logfile=name/"log.lammps"
        datfile=name/"sportran.dat"
        op_out = OPIO({
            "dat":datfile,
            "log":logfile,
        })
        return op_out

class MakeConfigurations(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "dump":Artifact(Path),
            "numb_lmp":int
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "name":List[str],
            "configurations":Artifact(List[Path]),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        dump =op_in["dump"]
        numb_lmp = int(op_in["numb_lmp"])
        oname=[]
        oconfigurations=[]
        data = dpdata.System(dump,"lammps/dump")
        l = len(data)
        for ii in range(0,numb_lmp):
            ofolder = Path(f"task.{ii:04d}")
            oname.append(str(ofolder))
            ofolder.mkdir()
            ofile= ofolder/"data.lammps"
            data[int(l/numb_lmp*ii)].to_lammps_lmp(ofile)
            oconfigurations.append(ofile)
        op_out = OPIO({
            "name":oname,
            "configurations":oconfigurations,
        })
        return op_out


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
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        dat = op_in["dat"]
        param = op_in["param"]
        energy_flux=None
        mass_flux=None
        TEMPERATURE=[]
        VOLUME =[]
        for d in dat:
            jfile = st.i_o.TableFile(d, group_vectors=True)
            jfile.read_datalines(start_step=0, NSTEPS=0, select_ckeys=['Temp','Volume', 'J', 'vcm[1]'])
            # energy_flux.append(jfile.data['J'])
            # mass_flux.append(jfile.data['vcm[1]'])
            if energy_flux is None:
                energy_flux = jfile.data['J']
            else:
                energy_flux = np.hstack((energy_flux,jfile.data['J']))
            if mass_flux is None:
                mass_flux = jfile.data['vcm[1]']
            else:mass_flux = np.hstack((mass_flux,jfile.data['vcm[1]']))
            TEMPERATURE.append(np.mean(jfile.data['Temp']))
            VOLUME.append(np.mean(jfile.data['Volume']))
        DT_FS = param['thermo_print_interval']*param['time_step']*1000                                # time step [fs]
        TEMPERATURE = np.mean(np.array(TEMPERATURE))  # temperature [K]
        VOLUME =np.mean(np.array(VOLUME))
        print('T = {:f} K'.format(TEMPERATURE))
        print('V = {:f} A^3'.format(VOLUME))
        j = st.HeatCurrent([energy_flux, mass_flux], UNITS='metal', DT_FS=DT_FS,TEMPERATURE=TEMPERATURE, VOLUME=VOLUME)
        FSTAR_THZ = j.Nyquist_f_THz*0.8
        jf, ax = j.resample(fstar_THz=FSTAR_THZ, plot=True, freq_units='thz')
        plt.xlim([0, 120])
        ax[1].set_ylim([0, 20]);
        plt.savefig("resampling_comparation.png")
        jf.cepstral_analysis()
        result = jf.cepstral_log
        with open("result.txt",'w') as f:
            f.writelines(result)
        result = Path("result.txt")
        op_out = OPIO({
            "output":result,
        })
        return op_out
