import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tcflow",
    version="0.0.17",
    author="Wenjie Zhang",
    author_email="gdbhcxhmjk@163.com",
    description="A framework for thermal conductivity calculation based on EMD&NEMD methods",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gdbhcxhmjk-z/ThermalConductivity-Workflow.git",
    packages=setuptools.find_packages(),
    install_requires=[
        "pydflow>=1.6.27",
        "lbg>=1.2.13",
        "dpdata>=0.2.7",
        "matplotlib>=3.6.3",
        "sportran",
        "numpy",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    provides=["tcflow"],
    script=[],
    entry_points={'console_scripts': [
         'tcflow = tcflow.submit:tc_main',
     ]}
)
