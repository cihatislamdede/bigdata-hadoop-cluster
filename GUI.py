import tkinter as tk
from tkinter import filedialog
import subprocess

root = tk.Tk()
root.geometry("800x600")
root.title("Big Data Hadoop")
root.configure(bg="lightsteelblue")

# centered title
title_label = tk.Label(
    root,
    text="Big Data Hadoop",
    font=("helvetica", 32, "bold"),
    bg="lightsteelblue",
)
title_label.grid(row=0, column=0, columnspan=3, padx=5, pady=5, sticky="nsew")


def upload_file():
    file_path = filedialog.askopenfilename()
    print("Selected file:", file_path)


def check_system_status():
    p = subprocess.Popen(
        ["docker", "exec", "-it", "hadoop-master", "jps"], stdout=subprocess.PIPE
    )
    output, _ = p.communicate()
    if "NameNode" in str(output):
        stop_dfs_button["state"] = tk.NORMAL
        start_dfs_button["state"] = tk.DISABLED
        alert_label = tk.Label(
            root, text="HDFS is running", font=("helvetica", 12), fg="green"
        )
        alert_label.grid(row=1, column=1, padx=5, pady=5)
    else:
        stop_dfs_button["state"] = tk.DISABLED
        start_dfs_button["state"] = tk.NORMAL
        alert_label = tk.Label(
            root, text="HDFS is not running", font=("helvetica", 12), fg="red"
        )
        alert_label.grid(row=1, column=1, padx=5, pady=5)
    root.after(2500, alert_label.destroy)


def stop_hdfs():
    stop_dfs_button["state"] = tk.DISABLED
    alert_label = tk.Label(
        root, text="Stopping HDFS...", font=("helvetica", 12), fg="green"
    )
    alert_label.grid(row=1, column=1, padx=5, pady=5)
    p = subprocess.Popen(
        [
            "docker",
            "exec",
            "-it",
            "hadoop-master",
            "bash",
            "-c",
            "/root/stop-hadoop.sh",
        ],
        stdout=subprocess.PIPE,
    )
    output, _ = p.communicate()
    print(output.decode("utf-8"))
    check_system_status()
    alert_label.destroy()


def start_hdfs():
    start_dfs_button["state"] = tk.DISABLED
    alert_label = tk.Label(
        root, text="Starting HDFS...", font=("helvetica", 12), fg="green"
    )
    alert_label.grid(row=1, column=1, padx=5, pady=5)
    p = subprocess.Popen(
        [
            "docker",
            "exec",
            "-it",
            "hadoop-master",
            "bash",
            "-c",
            "/root/start-hadoop.sh",
        ],
        stdout=subprocess.PIPE,
    )
    output, _ = p.communicate()
    print(output.decode("utf-8"))
    check_system_status()
    alert_label.destroy()


check_system_button = tk.Button(
    text="Check System\nStatus",
    command=check_system_status,
    bg="gray",
    fg="white",
    font=("helvetica", 12, "bold"),
    justify=tk.CENTER,
    anchor=tk.CENTER,
)
check_system_button.grid(row=1, column=0, padx=5, pady=5)

stop_dfs_button = tk.Button(
    text="Stop HDFS",
    command=stop_hdfs,
    bg="red",
    fg="white",
    font=("helvetica", 12, "bold"),
    state=tk.DISABLED,
    justify=tk.CENTER,
    anchor=tk.CENTER,
)
stop_dfs_button.grid(row=2, column=0, padx=5, pady=5)
start_dfs_button = tk.Button(
    text="Start HDFS",
    command=start_hdfs,
    bg="green",
    fg="white",
    font=("helvetica", 12, "bold"),
    state=tk.DISABLED,
    justify=tk.CENTER,
    anchor=tk.CENTER,
)
start_dfs_button.grid(row=2, column=1, padx=5, pady=5)
root.mainloop()
