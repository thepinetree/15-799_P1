import workload
import logging


def run_alg(workload_csv, timeout):
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    w = workload.Workload()
    w.setup(workload_csv)
    w.select()


def task_project1():
    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Starting action generation."',
            run_alg,
            'echo "Creating empty config file."',
            'echo \'\' > config.json',
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


def task_project1_setup():
    return {
        "actions": [
            'pip install psycopg',
            'pip install pandas',
            'pip install psutil',
            'pip install sqlparse',
            'pip install typing',
        ],
        # Always rerun this task.
        "uptodate": [False],
    }


if __name__ == "__main__":
    run_alg("./input/starter.csv", 0)
