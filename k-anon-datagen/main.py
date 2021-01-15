import json
import random
import string
import math
from time import gmtime, strftime
from common_tools.CsvWriter import *
from common_tools.name_gen import *
import os
import shutil

class VGHGen:
    vgh = None
    depth = None

    def __init__(self, vgh_initial, depth):
        self.vgh = vgh_initial
        self.depth = depth

    def getClusterBorders(self, t_len, count):
        mul = math.floor(t_len/count)
        ranges = []
        for c in range(count):
            ranges.append([mul * c, mul * (c + 1) - 1])

        if t_len%count:
            ranges[-1][1] = t_len-1

        return ranges

    def avg(self, lst):
        return [min(lst), max(lst)]

    def getRandomString(self, length):
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length))
        return result_str

    def avg_string(self, lst):
        max_str = max(lst)
        st = self.getRandomString(len(max_str))
        return st

    def generalize(self, values, level):
        result = {}
        values_cur = list(values)
        cluster_ranges = self.getClusterBorders(len(values_cur), level)
        for c in cluster_ranges:
            if type(values_cur[c[1]]) is int:
                avg = self.avg(values_cur[c[0]:c[1]+1])
                for i in range(c[0], c[1]+1):
                    values_cur[i] = avg
            if type(values_cur[c[1]]) is list:
                mins = [m[0] for m in values_cur[c[0]:c[1] + 1]]
                maxs = [m[1] for m in values_cur[c[0]:c[1]+1]]
                avg = [min(mins), max(maxs)]
                for i in range(c[0], c[1] + 1):
                    values_cur[i] = avg
            else:
                min_st = values_cur[c[0]]
                max_st = values_cur[c[1]]
                avg = [min_st, max_st]
                for i in range(c[0], c[1] + 1):
                    values_cur[i] = avg

        result[self.depth-level+1] = values_cur
        if level > 1:
            val_ret = self.generalize(values_cur, level-1)
            for key in val_ret:
                result[key] = val_ret[key]
        return result


    def generateVGH(self):
        for key in self.vgh:
            generalized = self.generalize(self.vgh[key][0], self.depth)
            for key_gen in generalized:
                self.vgh[key][key_gen] = generalized[key_gen]

            for key_gen in self.vgh[key]:
                for i in range(len(self.vgh[key][key_gen])):
                    if type(self.vgh[key][key_gen][i]) is list:
                        self.vgh[key][key_gen][i] = str(self.vgh[key][key_gen][i][0]) + "-" + str(self.vgh[key][key_gen][i][1])


class Generator:
    config = None
    table = []
    dataSources = None
    def __init__(self, config):
        self.config = None
        self.table = []
        self.dataSources = None

        self.config = config
        self.dataSources = [self.getRandomInt, self.getRandomInt]


    def getRandomString(self, length):
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length))
        return result_str

    def getRandomInt(self, len):
        range_start = 10 ** (len - 1)
        range_end = (10 ** len) - 1
        return random.randint(range_start, range_end)

    def getQidList(self, q_max, cols_count):
        qid_count = q_max
        if qid_count > cols_count:
            raise Exception("qid_count > cols_count")

        qid_idx = list(range(cols_count))
        random.shuffle(qid_idx)
        qid_idx = qid_idx[0:qid_count]
        qid_idx.sort()
        return qid_idx


    def generateRandomTable(self):
        cols_count = random.randrange(self.config["table"]["cols_min"], self.config["table"]["cols_max"]+1)
        rows_count = random.randrange(self.config["table"]["rows_min"], self.config["table"]["rows_max"]+1)

        header = []
        cols = []

        for col in range(cols_count):
            datasource = self.dataSources[random.randrange(0, 2)]

            # Количество повторяющихся значений атрибута
            repeated_count = random.randrange(self.config["table"]["min_repeat"], self.config["table"]["max_repeat"]+1)
            repeated_len = random.randrange(self.config["table"]["min_field_len"], self.config["table"]["max_field_len"]+1)
            rand_rep = datasource(repeated_len)
            col_repeated = [rand_rep for r in range(repeated_count)]

            col = []
            for row in range(rows_count-repeated_count):
                len_c = random.randrange(self.config["table"]["min_field_len"], self.config["table"]["max_field_len"] + 1)
                col.append(datasource(len_c))

            col.extend(col_repeated)
            col.sort()
            cols.append(col)

        # Заголовок
        for col in range(cols_count):
            datasource = self.getRandomString
            len_c = random.randrange(self.config["table"]["header_min"], self.config["table"]["header_max"] + 1)
            header.append(datasource(len_c))

        for row in range(rows_count):
            self.table.append([col[row] for col in cols])

        self.table.insert(0, header)

        qid_idx = self.getQidList(self.config["table"]["q_max"], cols_count)
        qid_list = []
        for i in qid_idx:
            qid_list.append([self.table[0][i], i])

        vgh = {}
        for qid in qid_list:
            vgh[qid[0]] = {}
            vgh[qid[0]][0] = [row[qid[1]] for row in self.table[1:]]

        vgh_gen = VGHGen(vgh, self.config["common"]["vgh_depth"])
        vgh_gen.generateVGH()

        return self.table, vgh


class Writer:
    table = None
    vgh = None
    config = None
    datafly_vgh_data = {}
    incognito_vgh = {}
    ds_name = None
    ds_path = None
    qi_selection = None

    def getRandomString(self, length):
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length))
        return result_str

    def __init__(self, table, vgh, config):
        self.table = None
        self.vgh = None
        self.config = None
        self.datafly_vgh_data = {}
        self.incognito_vgh = {}
        self.ds_name = None
        self.ds_path = None
        self.qi_selection = None

        self.table = table
        self.vgh = vgh
        self.config = config


    def delAttr(self, attrname):
        try:
            delattr(self, attrname)
        except AttributeError:
            pass

    def writeDataFlyVGH(self):  #txt
        vgh_folder = self.config["common"]["datafly_meta_folder"] + self.ds_name + "\\"
        for qi in self.vgh:
            self.datafly_vgh_data[qi] = {}
            self.datafly_vgh_data[qi]['path'] = {}
            self.datafly_vgh_data[qi]['path'] = vgh_folder + qi+"_"+self.ds_name+'.txt'

        for qi in self.datafly_vgh_data:
            self.datafly_vgh_data[qi]['data'] = []
            levels = []
            for level in self.vgh[qi]:
                levels.append([row for row in self.vgh[qi][level]])
            for row in range(len(levels[0])):
                self.datafly_vgh_data[qi]['data'].append([levels[col][row] for col in range(len(levels))])

        os.mkdir(vgh_folder)
        for qi in self.datafly_vgh_data:
            CsvWriter.WriteList(self.datafly_vgh_data[qi]['path'], self.datafly_vgh_data[qi]['data'])

    def writeIncognitoVGH(self): #json
        vgh_folder = self.config["common"]["incognito_meta_folder"] + self.ds_name + "\\"
        os.mkdir(vgh_folder)
        name_basic = vgh_folder + self.ds_name + '_'
        self.incognito_vgh['vgh'] = {}
        for s in self.qi_selection:
            self.incognito_vgh['vgh'][s] = { "path" : name_basic + str(s) + '_qi.json', "data" : {}}
            data = {}
            for key in self.qi_selection[s]:
                data[key] = self.vgh[key]
                for level in data[key]:
                    data[key][level] = [str(key) for key in data[key][level]]
            self.incognito_vgh['vgh'][s]['data'] = data
            with open(self.incognito_vgh['vgh'][s]['path'], 'w') as f:
                json.dump(data, f, indent=2)

    def prepareQiSelection(self):
        qi_all_list = [qi for qi in self.vgh]
        self.qi_selection = {}
        for i in range(len(qi_all_list)):
            self.qi_selection[i+1] = [qi for qi in qi_all_list[:i+1]]


    def getConfig(self, qi_names, k_val, df_vgh_paths, inc_qi_data):
        config = {
                "common":
                {
                    "table_path_base": self.config["common"]["dataset_folder"],
                    "table_name": self.ds_name + '.csv',
                    "qi_names": qi_names,
                    "k_value": k_val,
                    "output_path_base": self.config["common"]["output_path_base"],
                    "vgh_depth" : self.config["common"]["vgh_depth"]
                },
                "datafly":
                {
                    "vgh_data": df_vgh_paths
                },
                "incognito":
                {
                    "qi_data": inc_qi_data
                }
        }
        return config

    def writeConfigs(self):
        k_min = self.config["table"]["k_min"]
        k_max = self.config["table"]["k_max"]

        q_min = self.config["table"]["q_min"]
        q_max = self.config["table"]["q_max"]

        vgh_depth = self.config["common"]["vgh_depth"]

        configs_ready = []
        for k in range(k_min, k_max+1):
            for q in range(q_min, q_max+1):
                df_vgh_paths = [self.datafly_vgh_data[qi]['path'] for qi in self.qi_selection[q]]
                inc_vgh_path = self.incognito_vgh['vgh'][q]['path']
                config_current = self.getConfig(self.qi_selection[q], k, df_vgh_paths, inc_vgh_path)
                path = self.config["common"]["configs_folder"]
                path+= "config_" + self.ds_name + "_" + str(vgh_depth) + "vgh_" + str(k) + "k_" + str(q) + "q.json"
                configs_ready.append({"path" : path, "config" : config_current})

        for config in configs_ready:
            with open(config['path'], 'w') as f:
                json.dump(config['config'], f, indent=2)

    def getDsName(self):
        adj = random.randrange(0, 50)
        noun = random.randrange(0, 50)
        return nameGenerator.getName(adj, noun) + '_' + str(len(self.table[0])) + 'c_' + str(len(self.table)-1) + 'r_' + strftime("%d%m%H%M%S", gmtime())

    def writeAll(self):
        self.ds_name = self.getDsName()
        self.ds_path = self.config["common"]["dataset_folder"] + self.ds_name + '.csv'
        #os.mkdir(self.config["common"]["dataset_folder"])
        CsvWriter.WriteList(self.ds_path, self.table)
        self.prepareQiSelection()
        self.writeIncognitoVGH()
        self.writeDataFlyVGH()
        self.writeConfigs()


def cleanContents(path):
    print('Cleaning contents of ', path)
    shutil.rmtree(path, ignore_errors=True, onerror=None)

def clean(config):
    cleanContents(config["common"]["dataset_folder"])
    cleanContents(config["common"]["configs_folder"])
    cleanContents(config["common"]["incognito_meta_folder"])
    cleanContents(config["common"]["datafly_meta_folder"])
    os.mkdir(config["common"]["dataset_folder"])
    os.mkdir(config["common"]["configs_folder"])
    os.mkdir(config["common"]["incognito_meta_folder"])
    os.mkdir(config["common"]["datafly_meta_folder"])


def doJob(config):
    gen = Generator(config)
    table, vgh = gen.generateRandomTable()
    writer = Writer(table, vgh, config)
    writer.writeAll()
    del gen

def main():
    with open("config.json", "r") as file:
        config = json.load(file)

    clean(config)

    count_total = 1
    counter_iter = 0

    while counter_iter<count_total:
        vgh_min_override = 3
        vgh_max_override = 7
        vgh_delta = 1
        v = vgh_min_override
        while v<=vgh_max_override:
            rows_min_override = 1000
            rows_max_override = 10000
            rows_delta = 1000

            cols_min_override = config["table"]["cols_min"]
            cols_max_override = config["table"]["cols_max"]
            cols_delta = 1
            c = cols_min_override
            while c <= cols_max_override:
                r = rows_min_override
                while r <= rows_max_override:
                    print('Gen for r:', r, 'c:',c, 'vgh:', v)
                    config["common"]["vgh_depth"] = v
                    config["table"]["cols_min"] = c
                    config["table"]["cols_max"] = c
                    config["table"]["rows_min"] = r
                    config["table"]["rows_max"] = r
                    doJob(config)
                    r += rows_delta
                c += 1
            v+=vgh_delta
        counter_iter = counter_iter+1
    print('DONE')

if __name__ == '__main__':
    main()