import workload


def task_project1():
    def run_alg(workload_csv, _timeout):
        w = workload.Workload()
        w.setup(workload_csv)
        w.select()

    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Starting action generation."',
            run_alg,
        ],
        # Always rerun this task.
        "uptodate": [False],
        "verbosity": 2,
        "params": [
            {
                "name": "workload_csv",
                "long": "workload_csv",
                "help": "The PostgreSQL workload to optimize for.",
                "default": None,
            },
            {
                "name": "timeout",
                "long": "timeout",
                "help": "The time allowed for execution before this dodo task will be killed.",
                "default": None,
            },
        ],
    }


def project1_setup():
    return {
        "actions": [
            'pip install psycopg',
            'pip install pandas',
            'pip install sqlparse',
        ],
        # Always rerun this task.
        "uptodate": [False],
    }
