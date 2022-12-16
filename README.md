# Table of contents
- [About ThermalConductivity-Workflow](#ThermalConductivity-Workflow)

# About ThermalConductivity-Workflow
ThermalConductivity-Workflow is designed to compute thermal conductivity of different materials based on non-equilibrium&amp;equilibrium molecular dynamics simulations.It is supported by [dflow](https://github.com/deepmodeling/dflow), a Python framework for constructing scientific computing workflows.

# Environment Installation
* dflow, the necessary package in the host environment
* 
## Installation of dflow
With the power of dflow, users can easily minitor the whole workflow of ThermalConductivity tasks and dispatch their tasks to various computational resources. Before you use it, you should have dflow installed on your host computer (your PC or a remote server).

It it necessary to emphasize that, the computational nodes and monitor nodes are seperated. With dflow, you can deploy dflow and ThermalConductivity on your PC and achieve expensive computation on other resources (like Slurm and Cloud Platform) without any further effort.

Instructions of dflow installation are provided in detail on its [Github page](https://github.com/deepmodeling/dflow#Installdflow). Prerequisites of dflow usage are Docker and Kubenetes, where their main pages ([Docker](https://docs.docker.com/engine/install/) &amp; [Kubenetes](https://kubernetes.io/docs/tasks/tools/) include how you can install them. Besides, dflow repo also provides with easy-install shell scripts on dflow/scripts to install Docker &amp Kubenetes &amp dflow and make port-forwarding.
