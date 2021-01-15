import psycopg2
import os
import sys
from common_tools.AlgsRunner import Runner
from Mondrian.Mondrian import MondrianRunner
from Metrics.Metrics import *
import json

def AuxWriteCSV(fname, header, rows):
    with open(fname, 'w', encoding='utf8') as f:
        w = csv.DictWriter(f, header, dialect='excel', lineterminator='\n', delimiter=',', quotechar='"')
        w.writeheader()
        for row in rows:
            w.writerow(row)

class AuxCsvReader:
    @staticmethod
    def ReadCsvTable(path, parseHeader = False):
        with open(path, encoding='utf8') as csvfile:
            table_raw = list(filter(len, csv.reader(csvfile, delimiter=',')))

        header = list(table_raw[0])

        if not parseHeader:
            return table_raw
        else:
            return header, table_raw[1:]

class DBConnector:
    cursor = None
    conn = None

    def __init__(self, name, user, password, host):
        try:
            self.conn = psycopg2.connect(dbname=name, user=user,
                                         password=password, host=host)
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            print('Connected to', host)

        except Exception as e:
            print('Connection error')
            sys.exit()

        finally:
            pass

    def dumpTable(self, tableName):
        result = []
        self.cursor.execute("SELECT * FROM "+tableName)
        colnames = [desc[0] for desc in self.cursor.description]
        results = self.cursor.fetchall()
        for row in results:
            temp = {}
            for i in range(len(row)):
                temp[colnames[i]] = str(row[i])
            result.append(temp)
        return {'colnames' : colnames, 'dump' : result}

    def updateTable(self, tableName, data):
        self.cursor.execute("TRUNCATE "+tableName)
        for row in data:
            req_line = 'INSERT INTO '+tableName+' VALUES('
            for col in row:
                req_line = req_line + "'" + col + "',"
            req_line = req_line[:-1] + ');'
            self.cursor.execute(req_line)
        print('here')

class Framework:
    algs = {"Mondrian": MondrianRunner}

    io_dict = {"Mondrian": ...}

    config_path = None
    config = None

    def __init__(self, config_path):
        self.config_path = config_path
        with open(self.config_path, "r") as read_file:
            self.config = json.load(read_file)

        self.config['common']['table_path_base'] = os.path.dirname(os.path.realpath(__file__))
        self.config['common']['output_path_base'] = os.path.dirname(os.path.realpath(__file__))
        self.config['common']['table_name'] = self.config['dumper']['table_dump_path']
        t_name = self.config["common"]["table_name"]
        k_val = str(self.config["common"]["k_value"])+"k"
        q_cnt = str(len(self.config["common"]["qi_names"])) + "q"
        summary = [t_name, k_val, q_cnt]
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
        output_path = "metrics.json"

        with open(output_path, 'w') as outfile:
            json.dump(result, outfile, indent=4)


    def Run(self, verbose = False):
        self.RunAlgs(verbose)
        self.Measurements(verbose)



def main():
    config_path = 'config.json'
    with open(config_path, "r") as read_file:
        config = json.load(read_file)

    db_name = config['dumper']['db_name']
    db_user = config['dumper']['db_user']
    db_host = config['dumper']['db_host']
    db_password = config['dumper']['db_password']
    table_name = config['dumper']['table_name']
    table_dump_path = config['dumper']['table_dump_path']

    connection = DBConnector(db_name, db_user, db_password, db_host)

    result = connection.dumpTable(table_name)
    AuxWriteCSV(table_dump_path, result['colnames'], result['dump'])

    f = Framework(config_path)
    f.Run()

    table_ready = AuxCsvReader.ReadCsvTable(table_dump_path+'_anon', True)
    connection.updateTable(table_name, table_ready[1])

    print('here')

if __name__ == '__main__':
    main()