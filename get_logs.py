import subprocess
import os

NAMESPACE = "micro-lab5"
OUTPUT_DIR = "service_logs"

def get_pod_names(namespace):
    cmd = [
        "kubectl", "get", "pods", 
        "-n", namespace, 
        "-o", "jsonpath={.items[*].metadata.name}"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error fetching pods: {result.stderr}")
        return []
    return result.stdout.split()

def save_logs(namespace, pods, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for pod in pods:
        print(f"Downloading logs for: {pod}...")
        log_file_path = os.path.join(output_dir, f"{pod}.log")
        
        # Run kubectl logs
        cmd = ["kubectl", "logs", "-n", namespace, pod]
        with open(log_file_path, "w") as f:
            subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)

def main():
    pods = get_pod_names(NAMESPACE)
    if not pods:
        print("No pods found.")
        return
    
    save_logs(NAMESPACE, pods, OUTPUT_DIR)
    print(f"\nSuccess! Logs saved to the '{OUTPUT_DIR}' directory.")

if __name__ == "__main__":
    main()