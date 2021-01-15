from common_tools.AlgsRunner import Runner
from Mondrian.Mondrian import MondrianRunner
from Incognito.Incognito import IncognitoRunner
from Datafly.DataFly import DataFlyRunner
from Metrics.Metrics import *
from os import listdir
from os.path import isfile, join
import traceback
import gc
import threading
import time

class Framework:
    algs = {"Mondrian": MondrianRunner,
            "Incognito": IncognitoRunner,
            "Datafly": DataFlyRunner
            }

    io_dict = {"Mondrian": ...,
               "Incognito": ...,
               "Datafly": ...}

    config_path = None
    config = None

    def __init__(self, config_path):
        self.config_path = config_path
        with open(self.config_path, "r") as read_file:
            self.config = json.load(read_file)

        t_name = self.config["common"]["table_name"]
        k_val = str(self.config["common"]["k_value"])+"k"
        q_cnt = str(len(self.config["common"]["qi_names"])) + "q"
        vgh_depth = str(self.config["common"]["vgh_depth"])+"v"
        summary = [t_name, k_val, q_cnt, vgh_depth]
        print('\n-------')
        print(' '.join(summary))

    def RunAlgs(self, verbose):
        for alg in self.algs:
            runner = self.algs[alg](self.config_path)

            io = runner.Run()
            self.io_dict[alg] = io
            if verbose:
                print(alg + " : DONE")

    def Measurements(self, verbose):
        result = {  "Cavg": {},
                    "DM": {},
                    "GENILoss": {}}
        for alg in self.algs:
            cavg = CAvgMetric(self.io_dict[alg], self.config_path)
            cavg_value = cavg.calculate()

            dm = DMetric(self.io_dict[alg], self.config_path)
            dm_val = dm.calculate()

            loss = GenILossMetric(self.io_dict[alg], self.config_path)
            loss_val = loss.calculate()

            result["Cavg"][alg] = cavg_value
            result["DM"][alg] = dm_val
            result["GENILoss"][alg] = loss_val

        if verbose:
            for metric in result:
                for alg in result[metric]:
                    print(metric, " /", alg, ":", result[metric][alg])

        stub_runner = Runner(self.config_path)
        stub_runner.ReadConfig('stub')
        output_path = stub_runner.params_dir + stub_runner.params_str + "_metrics_.json"

        with open(output_path, 'w') as outfile:
            json.dump(result, outfile, indent=4)

        config_output_path = stub_runner.params_dir + stub_runner.params_str + "_used_config_.json"
        with open(config_output_path, 'w') as f:
            json.dump(self.config, f, indent=2)

    def Run(self, verbose = False):
        self.RunAlgs(verbose)
        self.Measurements(verbose)



def main():
    configs_path = "C:\\Users\\79313\\Documents\\repos\\k-anon-scratch\\configs\\generated\\"
    #configs_path = "C:\\Users\\79313\\Documents\\repos\\k-anon-scratch\\configs\\"
    config_files = [f for f in listdir(configs_path) if isfile(join(configs_path, f))]
    total = len(config_files)
    counter = 0
    for config in config_files:
        try:
            counter+=1
            print(str(counter)+'/'+str(total))
            f = Framework(configs_path+config)
            f.Run()
            del f
            gc.collect()
        except Exception as e:
            print('\n\n----------------------------------------')
            print(config, 'caused unhandled exception:' )
            print(traceback.format_exc())
            print('--------- Traceback ends here ----------\n\n')
        finally:
            continue

if __name__ == '__main__':
    main()
