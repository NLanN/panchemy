import os
import subprocess

def run_general_scripts():
    subprocess.call(
        "autoflake --remove-all-unused-imports --recursive --remove-unused-variables --in-place panchemy --exclude=__init__.py",
        shell=True)
    subprocess.call("black panchemy", shell=True)
    subprocess.call("isort --recursive --apply panchemy", shell=True)

def run_window_script():
    ...


def run_linux_script():
    ...


if __name__ == "__main__":
    run_general_scripts()
    if os.name == "nt":
        run_window_script()
    else:
        run_linux_script()
