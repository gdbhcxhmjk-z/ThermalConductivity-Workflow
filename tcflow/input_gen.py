import numpy as np
import time
import json

np.random.seed(int(time.time()))
np.random.randint(1,1000000)

def variable_initialization(file,T,dt,thermo_print_interval,traj_print_interval):
    file +=f"""
units metal
boundary p p p
atom_style atomic
#####################################################################
#                     Initial Conditons
#####################################################################
variable T equal {T} #[K] Temperature
variable DT equal {dt} #[ps] MD time steps
variable thermo_print_interval equal {thermo_print_interval} #[timesteps]
variable traj_print_interval equal {traj_print_interval} #[timesteps]
"""
    return file

def nemd_variable_initialization(file,T,dt,thermo_print_interval,traj_print_interval,T_diff,langevin_damp):
    file +=f"""
units metal
boundary p p p
atom_style atomic
#####################################################################
#                     Initial Conditons
#####################################################################
variable T equal {T} #[K] Temperature
variable diff equal {T_diff} #[K] Temperature difference
variable DT equal {dt} #[ps] MD time steps
variable langevin_damp equal {langevin_damp}
variable thermo_print_interval equal {thermo_print_interval} #[timesteps]
variable traj_print_interval equal {traj_print_interval} #[timesteps]
"""
    file +="""
variable        tlo equal ${T}-${diff}
variable        thi equal ${T}+${diff}
variable        cxhi equal 0.05*lx
variable        hxlo equal 0.5*lx
variable        hxhi equal 0.55*lx
    """
    return file

def make_structure(file,force_field,mass):
    file+="""
#####################################################################
#                       Read Structure
#####################################################################
read_data data.lammps
"""
    for l in force_field:
        file+=l+'\n'
    for i in range(len(mass)):
        file+=f"mass {i+1} {mass[i]} \n"
    return file

def energy_minimization(file):
    file+="""
#####################################################################
#                     Energy Minimization
#####################################################################
thermo    100
thermo_style  custom step press pe temp ke
dump    1 all atom 100 mini.lammpstrj
min_style  cg
minimize  1e-15 1e-15 5000 5000
undump    1
reset_timestep  0
"""
    return file

def velocity_initialization(file):
    np.random.seed(int(time.time()))
    s=np.random.randint(1,1000000)
    file+="""
#####################################################################
#                    Velocity Initialization
#####################################################################
"""
    file+="velocity all create ${T} "+f"{s} mom yes rot yes \n"
    return file

def NVT_equilibrium(file,steps):
    file+="""
#####################################################################
#                      NVT Equilibrium
#####################################################################
variable nvt_damp equal 100*${DT}
fix NVT all nvt temp ${T} ${T} ${nvt_damp}
timestep ${DT}
thermo_style custom step temp press vol
thermo ${thermo_print_interval}
dump NVT_equilibrium all custom ${traj_print_interval} NVT.lammpstrj id type x y z
"""
    file +=f"""run {steps}
unfix NVT
undump NVT_equilibrium
reset_timestep  0
"""
    return file

def compute_flux(file,flag,NVE_steps):
    file +="""
#####################################################################
#                 NVE Simulation: Production
#####################################################################

#Compute heat flux & mass mass_flux
compute KE all ke/atom
compute PE all pe/atom
"""
    if(flag):
        file+="compute V all stress/atom NULL virial"
    else:
        file+="compute V all centroid/stress/atom NULL virial"
    file+="""
compute J all heat/flux KE PE V
compute cc1 all chunk/atom type
compute vcm all vcm/chunk cc1

#Output Settings
thermo       ${thermo_print_interval}
thermo_style custom step time temp pe ke etotal press vol c_J[1] c_J[2] c_J[3] c_vcm[1][1] c_vcm[1][2] c_vcm[1][3]
## The following line can be uncommented to print the trajectory.
## sportran.i_o.LAMMPSLogFile will detect this line containing "DUMP_RUN" in the log.lammps file,
## it will extract the desired columns, and save the data in numpy binary format
#dump         DUMP_RUN all custom ${traj_print_interval} silica-run.lammpstrj id type xu yu zu vx vy vz
fix          NVE_RUN all nve
"""
    file +=f"""run {NVE_steps}
unfix        NVE_RUN
#undump       DUMP_RUN
write_restart pentacene-run.rst
write_data pentacene-run.init
"""
    return file

def NEMD_equilibrium(file,NEMD_equilibrium_steps):
    file +="""
#####################################################################
#                 NEMD Simulation: Equilibrium
#####################################################################

#select hot/cold region
region          hot block ${hxlo} ${hxhi} INF INF INF INF
region          cold block  0 ${cxhi} INF INF INF INF
compute         Thot all temp/region hot
compute         Tcold all temp/region cold

fix             1 all nve
"""
    np.random.seed(int(time.time()))
    s1=np.random.randint(1,1000000)
    s2=np.random.randint(1,1000000)
    file+="fix hot all langevin ${thi} ${thi} ${langevin_damp}"+ f" {s1} tally yes \n"
    file+="fix cold all langevin ${tlo} ${tlo} ${langevin_damp}"+ f" {s2} tally yes \n"
    file+="""
fix_modify      hot temp Thot
fix_modify      cold temp Tcold

variable        tdiff equal c_Thot-c_Tcold
thermo_style    custom step temp c_Thot c_Tcold f_hot f_cold v_tdiff
thermo_modify   colname c_Thot Temp_hot colname c_Tcold Temp_cold &
                colname f_hot E_hot colname f_cold E_cold &
                colname v_tdiff dTemp_step
thermo          ${thermo_print_interval}

"""
    file +=f"""run {NEMD_equilibrium_steps}
reset_timestep  0
"""
    return file

def NEMD_production(file,NEMD_production_steps):
    file +="""
#####################################################################
#                 NEMD Simulation: Production
#####################################################################

# thermal conductivity calculation
# reset langevin thermostats to zero energy accumulation
compute ke all ke/atom # atomic kinetic energy
variable kb equal 8.625e-5 # boltzmann constant, eV/K
variable temp atom c_ke/1.5/${kb} # atomic temperature

"""
    np.random.seed(int(time.time()))
    s1=np.random.randint(1,1000000)
    s2=np.random.randint(1,1000000)
    file+="fix hot all langevin ${thi} ${thi} ${langevin_damp} "+ f" {s1} tally yes \n"
    file+="fix cold all langevin ${tlo} ${tlo} ${langevin_damp} "+ f" {s2} tally yes \n"
    file+="""
fix_modify      hot temp Thot
fix_modify      cold temp Tcold
compute         layers all chunk/atom bin/1d x lower 0.02 units reduced
fix		NEMD_RUN all ave/chunk 10 1000 10000 layers v_temp file temp_profile.txt
"""
    file +=f"""run {NEMD_production_steps}
"""
    return file


def NVT_input(param):
    NVT ="#NVT simulation for different configurations\n"
    NVT = variable_initialization(NVT,param['temperature'],param['time_step'],param['thermo_print_interval'],param['traj_print_interval'])
    NVT = make_structure(NVT,param['load_force_field'],param['mass_map'])
    NVT = energy_minimization(NVT)
    NVT = velocity_initialization(NVT)
    num = param.get("num_configurations",None) if param.get("num_configurations",None) else 0
    steps = np.max((num*param['traj_print_interval']*10,200000))
    NVT = NVT_equilibrium(NVT,steps)
    with open("NVT.lammps",'w') as fp:
        fp.write(NVT)

def NVE_input(param):
    NVE ="#NVE simulation for heat flux\n"
    NVE = variable_initialization(NVE,param['temperature'],param['time_step'],param['thermo_print_interval'],param['traj_print_interval'])
    NVE = make_structure(NVE,param['load_force_field'],param['mass_map'])
    NVE = energy_minimization(NVE)
    NVE = velocity_initialization(NVE)
    NVE = NVT_equilibrium(NVE,param['NVT_steps'])
    NVE = compute_flux(NVE,param['is_two-body-potential'],param['NVE_steps'])
    with open("in.lammps",'w') as fp:
        fp.write(NVE)

def NEMD_input(param):
    NEMD = "#NEMD simulation for temperature gradient\n"
    NEMD = nemd_variable_initialization(NEMD,param['temperature'],param['time_step'],param['thermo_print_interval'],param['traj_print_interval'],param['temperature_difference'],param['langevin_damp'])
    NEMD = make_structure(NEMD,param['load_force_field'],param['mass_map'])
    NEMD = energy_minimization(NEMD)
    NEMD = velocity_initialization(NEMD)
    NEMD = NVT_equilibrium(NEMD,param['NVT_steps'])
    NEMD = NEMD_equilibrium(NEMD,param['NEMD_equilibrium_steps'])
    NEMD = NEMD_production(NEMD,param['NEMD_production_steps'])
    with open("in.lammps",'w') as fp:
        fp.write(NEMD)

if __name__ == "__main__":
    with open("parameters.json",'r') as fp :
        param = json.load(fp)
    # NVT_input(param)
    # NVE_input(param)
    NEMD_input(param)
