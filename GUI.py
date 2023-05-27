import tkinter as tk
import subprocess
from tkinter import filedialog, ttk, scrolledtext


class App:
    def __init__(self, root: tk.Tk, **kwargs):
        self.tabs = kwargs.get("tabs", [])
        self.index_dict = kwargs.get("index_dict", {})
        self.selected_file_path = ""

        root.title("Big Data Project")
        self.bg_color = "#26242f"
        style = ttk.Style()
        style.theme_use("clam")
        root.configure(background=self.bg_color)

        # set window size
        width = 800
        height = 800
        screenwidth = root.winfo_screenwidth()
        screenheight = root.winfo_screenheight()
        alignstr = "%dx%d+%d+%d" % (
            width,
            height,
            (screenwidth - width) / 2,
            (screenheight - height) / 2,
        )
        root.geometry(alignstr)

        # system status section
        label = tk.Label(
            root,
            text="System Status",
            font=("Arial", 14),
            fg="white",
            background=self.bg_color,
        )
        label.grid(row=0, column=0, padx=5, pady=10)
        system_status_button = tk.Button(
            root,
            text="Check System\nStatus",
            command=self.check_system_status,
            background="lightblue",
            fg="black",
        )
        system_status_button.grid(row=1, column=0, padx=5, pady=10)

        self.stop_hdfs_button = tk.Button(
            root,
            text="Stop HDFS",
            command=self.stop_hdfs,
            state=tk.DISABLED,
            background="red",
            fg="black",
        )
        self.stop_hdfs_button.grid(row=1, column=1, padx=5, pady=10)
        self.start_hdfs_button = tk.Button(
            root,
            text="Start HDFS",
            command=self.start_hdfs,
            state=tk.DISABLED,
            background="lightgreen",
            fg="black",
        )
        self.start_hdfs_button.grid(row=1, column=2, padx=5, pady=10)
        sep = ttk.Separator(root, orient="horizontal")
        sep.grid(row=2, columnspan=3, sticky="ew", padx=5, pady=10)

        # file upload section
        label2 = tk.Label(
            root,
            text="Upload File",
            font=("Arial", 14),
            fg="white",
            background=self.bg_color,
        )
        label2.grid(row=3, column=0, padx=5, pady=10)
        select_file_button = tk.Button(
            root,
            text="Select File",
            command=self.select_file,
            background="lightblue",
            fg="black",
        )
        select_file_button.grid(row=4, column=0, padx=5, pady=10)
        self.seleceted_file_label = tk.Label(
            root,
            text="",
            font=("Arial", 10),
            fg="white",
            background=self.bg_color,
        )
        self.seleceted_file_label.grid(row=5, column=0, padx=5, pady=0)
        self.upload_file_button = tk.Button(
            root,
            text="Upload",
            command=lambda: self.upload_file_to_hdfs(self.selected_file_path),
            background="green",
            fg="black",
            state=tk.DISABLED,
        )
        self.upload_file_button.grid(row=4, column=1, padx=5, pady=10)
        sep2 = ttk.Separator(root, orient="horizontal")
        sep2.grid(row=6, columnspan=3, sticky="ew", padx=5, pady=10)

        # map reduce section
        label3 = tk.Label(
            root,
            text="Map Reduce Functions",
            font=("Arial", 14),
            fg="white",
            background=self.bg_color,
        )
        label3.grid(row=8, column=0, padx=5, pady=10)

        file_name_input_label = tk.Label(
            root,
            text="Select File",
            font=("Arial", 10),
            fg="white",
            background=self.bg_color,
        )
        file_name_input_label.grid(row=9, column=0, padx=5, pady=10)
        self.file_name_input = ttk.Combobox(
            root,
            state="readonly",
            width=18,
            values=self.get_uploaded_files_from_hdfs(),
        )
        self.file_name_input.grid(row=10, column=0, padx=5, pady=10)
        self.output_name_input_label = tk.Label(
            root,
            text="Output Name",
            font=("Arial", 10),
            fg="white",
            background=self.bg_color,
        )
        self.output_name_input_label.grid(row=9, column=1, padx=5, pady=10)
        self.output_name_input = tk.Entry(root, width=20)
        self.output_name_input.grid(row=10, column=1, padx=5, pady=10)

        key_idx_dropdown = ttk.Combobox(
            root,
            values=list(self.index_dict.keys()),
            state="readonly",
            width=18,
        )
        key_idx_dropdown_label = tk.Label(
            root,
            text="Key Index",
            font=("Arial", 10),
            fg="white",
            background=self.bg_color,
        )
        key_idx_dropdown.grid(row=12, column=0, padx=5, pady=10)
        key_idx_dropdown_label.grid(row=11, column=0, padx=5, pady=10)
        value_idx_dropdown = ttk.Combobox(
            root,
            values=list(self.index_dict.keys()),
            state="readonly",
            width=18,
        )
        value_idx_dropdown_label = tk.Label(
            root,
            text="Value Index",
            font=("Arial", 10),
            fg="white",
            background=self.bg_color,
        )
        value_idx_dropdown.grid(row=12, column=1, padx=5, pady=10)
        value_idx_dropdown_label.grid(row=11, column=1, padx=5, pady=10)
        # start map reduce button
        self.start_map_reduce_button = tk.Button(
            root,
            text="Start Map Reduce",
            command=lambda: self.start_map_reduce(
                input_file_name=self.file_name_input.get(),
                output_file_name=self.output_name_input.get(),
                key_index=key_idx_dropdown.get(),
                value_index=value_idx_dropdown.get(),
            ),
            background="lightgreen",
            fg="black",
        )
        self.start_map_reduce_button.grid(row=13, column=0, padx=5, pady=10)
        self.map_reduce_output_label = scrolledtext.ScrolledText(
            root,
            width=30,
            height=10,
            font=("Times New Roman", 10),
            fg="white",
            background=self.bg_color,
            wrap=tk.WORD,
        )
        self.map_reduce_output_label.grid(row=13, column=1, padx=5, pady=10)
        self.check_system_status()

    def toast_notification(
        self, message: str, duration: int = 1500, color: str = "lightgreen"
    ):
        toast = tk.Message(
            root,
            text=f"ⓘ {message}",
            padx=5,
            pady=5,
            width=200,
            justify="center",
            relief=tk.RIDGE,
            background=color,
        )
        toast.grid(row=1, column=3, padx=5, pady=10)
        toast.after(duration, toast.destroy)

    def select_file(self):
        self.selected_file_path = filedialog.askopenfilename()
        self.upload_file_button["state"] = tk.NORMAL
        self.upload_file_button.update()
        self.seleceted_file_label["text"] = self.selected_file_path.split("/")[-1]

    def upload_file_to_hdfs(self, path: str):
        if self.selected_file_path == "":
            self.toast_notification("Please select a file", color="red")
            return
        self.upload_file_button["state"] = tk.DISABLED
        self.upload_file_button["text"] = "Uploading..."
        self.upload_file_button.update()
        file_name = path.split("/")[-1]
        p = subprocess.Popen(
            ["docker", "cp", path, "hadoop-master:/root/input/"], stdout=subprocess.PIPE
        )
        p.communicate()
        self.toast_notification(f"{file_name} copied to container", duration=500)
        p = subprocess.Popen(
            [
                "docker",
                "exec",
                "-it",
                "hadoop-master",
                "bash",
                "-c",
                f"hdfs dfs -put /root/input/{file_name} input/",
            ],
            stdout=subprocess.PIPE,
        )
        p.communicate()
        self.upload_file_button["state"] = tk.NORMAL
        self.upload_file_button["text"] = "Upload"
        self.upload_file_button.update()
        self.toast_notification(f"{file_name} uploaded to HDFS", duration=1500)
        self.update_file_name_input_values()

    def check_system_status(self):
        p = subprocess.Popen(
            ["docker", "exec", "-it", "hadoop-master", "jps"], stdout=subprocess.PIPE
        )
        output, _ = p.communicate()
        if "NameNode" in str(output):
            self.toast_notification("HDFS is running", duration=1000)
            self.stop_hdfs_button["state"] = tk.NORMAL
            self.start_hdfs_button["state"] = tk.DISABLED
        else:
            self.toast_notification("HDFS is not running", duration=1000, color="red")
            self.stop_hdfs_button["state"] = tk.DISABLED
            self.start_hdfs_button["state"] = tk.NORMAL

    def stop_hdfs(self):
        self.stop_hdfs_button["state"] = tk.DISABLED
        self.stop_hdfs_button["text"] = "Stopping HDFS..."
        self.stop_hdfs_button.update()
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
        p.communicate()
        self.stop_hdfs_button["text"] = "Stop HDFS"
        self.start_hdfs_button["state"] = tk.NORMAL
        self.stop_hdfs_button.update()
        self.start_hdfs_button.update()
        self.toast_notification("HDFS stopped", duration=1000)
        # print(output.decode("utf-8"))
        self.update_file_name_input_values()

    def start_hdfs(self):
        self.start_hdfs_button["state"] = tk.DISABLED
        self.start_hdfs_button["text"] = "Starting HDFS..."
        self.start_hdfs_button.update()
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
        p.communicate()
        self.start_hdfs_button["text"] = "Start HDFS"
        self.stop_hdfs_button["state"] = tk.NORMAL
        self.start_hdfs_button.update()
        self.stop_hdfs_button.update()
        self.toast_notification("HDFS started", duration=1500)
        # print(output.decode("utf-8"))
        self.update_file_name_input_values()

    def get_uploaded_files_from_hdfs(self):
        p = subprocess.Popen(
            ["docker", "exec", "-it", "hadoop-master", "hdfs", "dfs", "-ls", "input/"],
            stdout=subprocess.PIPE,
        )
        output, _ = p.communicate()
        output = output.decode("utf-8").split("\n")
        output = [i.split(" ")[-1] for i in output if i != ""]
        output = [i.split("/")[-1].replace("\r", "") for i in output]
        if output == ["ConnectionRefused"]:
            output = ["Please start HDFS"]
        return output

    def update_file_name_input_values(self):
        self.file_name_input["values"] = self.get_uploaded_files_from_hdfs()
        self.file_name_input.update()

    def start_map_reduce(self, **kwargs):
        print("Starting map reduce")
        print(kwargs)


if __name__ == "__main__":
    root = tk.Tk()
    tabs = ["System", "Upload File", "MapReduce Jobs"]
    index_dict = {
        "Id": 0,
        "Name": 1,
        "RatingDist1": 2,
        "pagesNumber": 3,
        "RatingDist4": 4,
        "RatingDistTotal": 5,
        "PublishMonth": 6,
        "PublishDay": 7,
        "Publisher": 8,
        "CountsOfReview": 9,
        "PublishYear": 10,
        "Language": 11,
        "Authors": 12,
        "Rating": 13,
        "RatingDist2": 14,
        "RatingDist5": 15,
        "ISBN": 16,
        "RatingDist3": 17,
        "Count of text reviews": 18,
        "PagesNumber": 19,
    }
    app = App(root, tabs=tabs, index_dict=index_dict)
    root.mainloop()
