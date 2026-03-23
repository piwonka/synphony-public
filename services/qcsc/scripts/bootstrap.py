import sys
import os
import random
import numpy as np
import mathutils.noise
import bpy  # import may be flagged as an error, dependency only available at runtime

# Read job information from stdin and split it into its parts


def read_job():
    line = sys.stdin.readline().strip()
    if not line:
        return None

    parts = line.split()
    if len(parts) != 3:
        print(f"ERROR: malformed job line: {line}", flush=True)
        return None

    if parts[0] != "JOB":
        print(f"ERROR: expected 'JOB', got: {parts[0]}", flush=True)
        return None

    # unpack fields
    _, input_file, seed = parts
    return input_file, seed


# The main function that keeps blender running and waits for jobs to be executed
if __name__ == '__main__':
    # read user script path from console args
    user_script_path = sys.argv[sys.argv.index("--f")+1]
    while True:  # repeat until death
        job = read_job()  # read job from stdin
        if job is None:
            continue

        inputFile, seed = job  # split the job into parts
        bpy.ops.wm.open_mainfile(filepath=inputFile)  # load the scene
        wrapper_globals = {  # ensure user script has all necessary args from the job using globals
            "__name__": "__main__",
            "__file__": user_script_path,
            "INPUT_FILE": inputFile,
            "seed": seed,
        }
        # load and run all user scripts from the specified path
        user_scripts = [
            f for f in os.listdir(user_script_path)
            if not f.startswith("bootstrap") and f.endswith(".py")
        ]

        for file in user_scripts:
            print(file)
            with open(file, 'r') as f:
                code = compile(f.read(), user_script_path, 'exec')
                exec(code, wrapper_globals)
        # Once all user scripts have propagated a result message, print a done flag to ensure orchestrator knows this work item has finished all blender workflows
        print("DONE"+","+seed+","+inputFile, flush=True)
