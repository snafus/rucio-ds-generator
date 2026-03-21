# Compatibility shim for editable installs on Python 3.6 / pip<22.
# On older toolchains pip falls back to the legacy setup.py path and does
# not read metadata, package discovery, or entry_points from pyproject.toml.
# Everything needed for a working install is declared here explicitly.
# Authoritative metadata (dependencies, classifiers, etc.) lives in pyproject.toml
# and is used by modern pip automatically.
from setuptools import find_packages, setup

setup(
    name="rucio-ds-generator",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "rucio-clients",
        "PyYAML",
        "tqdm",
        "requests",
        "python-dotenv>=0.20,<1.0",  # 1.0+ requires Python 3.8+
        "Jinja2",
    ],
    extras_require={
        "test": [
            "pytest<7",       # 7.0+ dropped Python 3.6
        ],
        "dev": [
            "pytest<7",       # 7.0+ dropped Python 3.6
            "flake8<6",       # 6.0+ dropped Python 3.6
            "black<22",       # 22.0+ dropped Python 3.6
            "isort<5.12",     # 5.12+ dropped Python 3.6
            "mypy<0.981",     # 0.981+ dropped Python 3.6
        ],
    },
    entry_points={
        "console_scripts": [
            "dataset-generator = dataset_generator.__main__:main",
        ],
    },
)
