from pathlib import Path
from runpy import run_path

from setuptools import setup


hooks = run_path(str(Path(__file__).with_name("build_support.py")))
setup(
    cmdclass={
        "bdist_wheel": hooks["CleanBdistWheel"],
        "build_py": hooks["IdentityBuildPy"],
        "sdist": hooks["IdentitySdist"],
    }
)
