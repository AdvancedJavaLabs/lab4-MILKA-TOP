import subprocess
import time
import csv
import os

input_path = "./input"
output_base_path = "./output"
jar_file = "out/artifacts/main_main_jar/main.main.jar"
driver_class = "ru.itmo.SalesDriver"
num_reducers = [1, 4, 16]
split_sizes = [16, 128, 256, 1024]
hadoop_cmd = "hadoop"
results_file = "hadoop_performance_results.csv"


def run_hadoop_job(reducers, split_size, output_path):
    try:
        subprocess.run(
            [
                hadoop_cmd, "jar", jar_file, driver_class,
                input_path, output_path, str(reducers), str(split_size * 1024 * 1024)
            ],
            check=True
        )

        log_file_path = os.path.join(output_path, "execution_time.log")

        if not os.path.exists(log_file_path):
            raise FileNotFoundError(f"Log file not found: {log_file_path}")

        with open(log_file_path, "r") as log_file:
            content = log_file.read().strip()

        return float(content)
    except subprocess.CalledProcessError as e:
        print(f"Error running Hadoop job: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


def main():
    results = []

    results.append(["Num Reducers", "Split Size (MB)", "Execution Time (s)"])

    for reducers in num_reducers:
        for split_size in split_sizes:
            output_path = output_base_path
            print(f"Running job with {reducers} reducers and split size {split_size} MB...")
            exec_time = run_hadoop_job(reducers, split_size, output_path)
            if exec_time is not None:
                print(f"Completed in {exec_time} seconds.")
                results.append([reducers, split_size, exec_time])
            else:
                print("Job failed.")

    with open(results_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(results)

    print(f"Results saved to {results_file}")

if __name__ == "__main__":
    main()
