import os
import subprocess


def run_general_scripts():
    subprocess.call("mypy panchemy", shell=True)
    subprocess.call("panchemy --check", shell=True)
    subprocess.call("isort - -recursive - -check - only panchemy", shell=True)
    subprocess.call("flake8", shell=True)


def run_window_scripts():
    ...


def run_linux_scripts():
    ...


if __name__ == "__main__":
    run_general_scripts()
    if os.name == "nt":
        run_window_scripts()
    else:
        run_linux_scripts()
