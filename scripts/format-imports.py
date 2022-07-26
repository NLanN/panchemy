import os
import subprocess


def run_general_scripts():
    subprocess.call("isort --recursive  --force-single-line-imports --apply panchemy",
                    shell=True)


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
