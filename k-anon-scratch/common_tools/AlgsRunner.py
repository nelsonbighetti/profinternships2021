import json
import os
from common_tools.CsvWriter import CsvWriter

class Runner:
    config = None
    config_path = None

    def __init__(self, config_path):
        self.config_path = config_path

    def GetOutputPath(self, alg_name):
        self.table_name_no_ext = self.table_name.split('.')[0]
        table_name_ext = self.table_name.split('.')[1]

        # dataset dir
        self.dataset_dir = self.output_path_base + self.table_name_no_ext + "\\"

        # params
        self.params_str = str(self.k_value) + "k_" + str(len(self.qi_names)) + "q_"  + str(self.vgh_depth) + "v"

        # params dir
        self.params_dir = self.dataset_dir + self.params_str + "\\"

        # dataset name
        self.output_file = self.table_name_no_ext + "_"

        # alg_name + params
        self.output_file += alg_name + "_" + str(self.k_value) + "k_" + str(len(self.qi_names)) + "q_" + str(self.vgh_depth) + "v"

        # ext
        self.output_file += "."+table_name_ext

    def ReadConfig(self, alg_name):
        with open(self.config_path, "r") as read_file:
            self.config = json.load(read_file)

        self.table_path_base = self.config["common"]["table_path_base"]
        self.table_name = self.config["common"]["table_name"]
        self.qi_names = self.config["common"]["qi_names"]
        self.k_value = self.config["common"]["k_value"]
        self.output_path_base = self.config["common"]["output_path_base"]
        self.vgh_depth = self.config["common"]["vgh_depth"]
        self.GetOutputPath(alg_name)

    def WriteOutput(self, data):
        if not os.path.exists(self.dataset_dir):
            os.mkdir(self.dataset_dir)

        if not os.path.exists(self.params_dir):
            os.mkdir(self.params_dir)

        self.output_path = self.params_dir + self.output_file
        CsvWriter.WriteList(self.output_path, data)