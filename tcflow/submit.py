

from dflow import config, s3_config
from dflow.plugins import bohrium
from dflow.plugins.bohrium import TiefblueClient
from monty.serialization import loadfn
import argparse
import sys
sys.path.append("..")
sys.path.append("../tcflow")
config["host"] = "https://workflows.deepmodeling.com"
config["k8s_api_server"] = "https://workflows.deepmodeling.com"

machine_param = loadfn("machine.json")
email = machine_param.get("email",None)
password = machine_param.get("password",None)
program_id = machine_param.get("program_id",None)
bohrium.config["username"] = email
bohrium.config["password"] = password
bohrium.config["program_id"] = program_id
s3_config["repo_key"] = "oss-bohrium"
s3_config["storage_client"] = TiefblueClient()
from tcflow.EMD_run import EMD_run_main
from tcflow.NEMD_run import NEMD_run_main

def tc_main():
    parser = argparse.ArgumentParser(description='EMD&NEMD Thermal Conductivity Workflow')
    parser.add_argument("--emd", help="Using EMD method to calculate Thermal Conductivity",
                        action="store_true")
    parser.add_argument("--nemd", help="Using NEMD method to calculate Thermal Conductivity",
                        action="store_true")
    args = parser.parse_args()

    if args.emd:
        EMD_run_main()
    elif args.nemd:
        NEMD_run_main()

if __name__ == "__main__":
    tc_main()
