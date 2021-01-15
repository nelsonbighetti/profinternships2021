import json
from common_tools.CsvReadAndConvert import *


import collections
class GenILossMetric:
    io_data = None
    config = None
    def __init__(self, io_data, config_path):
        self.io_data = io_data
        with open(config_path, "r") as read_file:
            self.config = json.load(read_file)

    def defineUpperLowerBounds(self, table, attrNum, isOriginal):
        if (isOriginal):
            li_current = []
            for row in table:
                li_current.append(row[attrNum])
            return {"Lo" : min(li_current), "Hi" : max(li_current)}
        else:
            li_current = []
            for entry in table:
                if isinstance(entry[attrNum], list):
                    li_current.append({"Lo" : entry[attrNum][0], "Hi" : entry[attrNum][1]})
                else:
                    li_current.append({"Lo" : entry[attrNum], "Hi" : entry[attrNum]})

            return li_current

    def calculate(self):
        header_orig, table_orig = CsvReader.ReadCsvTable(self.io_data["in"], True, True)
        header_anon, table_anon = CsvReader.ReadCsvTable(self.io_data["out"], True, True)


        T = len(table_anon) # количество записей в анонимизированной таблице
        n = len(header_orig) # количество атрибутов
        UL_ij = []  # запись ячейки для данного атрибута, обобщенная до интервала
        UL_i = []   # верхняя и нижняя граница атрибута
        for attr_idx in range(len(header_orig)):
            if(header_orig[attr_idx]!='admission' and header_orig[attr_idx]!='discharge'):
                UL_ij.append(self.defineUpperLowerBounds(table_anon, attr_idx, False)) # Определение границ интервалов
                UL_i.append(self.defineUpperLowerBounds(table_orig, attr_idx, True))   # Определение оригинальных границ
            else:
                UL_ij.append(self.defineUpperLowerBounds(table_anon, 0, False)) # Определение границ интервалов
                UL_i.append(self.defineUpperLowerBounds(table_orig, 0, True))   # Определение оригинальных границ

        # Множитель
        multiplicator = 1 / T * n

        # Сумма
        sigma = 0.0

        for i in range(n):
            for j in range(T):
                numerator = UL_ij[i][j]["Hi"] - UL_ij[i][j]["Lo"]
                denominator = UL_i[i]["Hi"] - UL_i[i]["Lo"]
                sigma+=numerator/denominator

        result = multiplicator * sigma
        return result

class DMetric:
    io_data = None
    config = None

    def __init__(self, io_data, config_path):
        self.io_data = io_data
        with open(config_path, "r") as read_file:
            self.config = json.load(read_file)

    def findEq(self, table, qid_idx):
        table_qid_only = []
        for row_original in table:
            row = str([col for col in row_original if row_original.index(col) in qid_idx])
            table_qid_only.append(row)

        eq_list = collections.Counter(table_qid_only)
        return eq_list


    def calculate(self):
        header_orig, table_orig = CsvReader.ReadCsvTable(self.io_data["in"], True, False)
        header_anon, table_anon = CsvReader.ReadCsvTable(self.io_data["out"], True, False)

        qid_idx = [i for i, e in enumerate(header_anon) if e in self.config['common']['qi_names']]

        EQList = self.findEq(table_anon, qid_idx)

        T = len(table_orig)
        DMT = 0

        for eq_key in EQList:
           if (EQList[eq_key] >= self.config["common"]["k_value"]):
               DMT += EQList[eq_key] * EQList[eq_key]
           else:
               DMT += EQList[eq_key] * T

        return DMT

class CAvgMetric:
    io_data = None
    config = None

    def __init__(self, io_data, config_path):
        self.io_data = io_data
        with open(config_path, "r") as read_file:
            self.config = json.load(read_file)

    def findEq(self, table, qid_idx):
        table_qid_only = []
        for row_original in table:
            row = str([col for col in row_original if row_original.index(col) in qid_idx])
            table_qid_only.append(row)

        eq_list = collections.Counter(table_qid_only)
        return eq_list

    def calculate(self):
        header_orig, table_orig = CsvReader.ReadCsvTable(self.io_data["in"], True, False)
        header_anon, table_anon = CsvReader.ReadCsvTable(self.io_data["out"], True, False)

        qid_idx = [i for i, e in enumerate(header_anon) if e in self.config['common']['qi_names']]


        T = len(table_orig)
        total_eq = len(self.findEq(table_anon, qid_idx))

        CAvg = T/(total_eq*self.config['common']['k_value'])

        return CAvg
