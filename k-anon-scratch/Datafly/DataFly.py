import csv
from common_tools.AlgsRunner import Runner

class DataFly():
    def __init__(self, records):
        self.records = records
        
    def anonymize(self, header,  qi_names, qi_files, k):
        self.ATTNAME = header
        self.confile = qi_files
        domains, gen_levels = {}, {}
        qi_frequency = {}       # store the frequency for each qi value
        assert len(self.confile) == len(qi_names), 'number of config files  not equal to number of QI-names'
        generalize_tree = dict()
        for idx, name in enumerate(qi_names):
            generalize_tree[name] = Tree(self.confile[idx])

        for qiname in qi_names:
            domains[qiname] = set()
            gen_levels[qiname] = 0
        
        for idx, record in enumerate(self.records):
            qi_sequence = self._get_qi_values(record[:], qi_names, generalize_tree)
            
            if qi_sequence in qi_frequency:
                qi_frequency[qi_sequence].add(idx)
            else:
                qi_frequency[qi_sequence] = {idx}
                for j, value in enumerate(qi_sequence):
                    domains[qi_names[j]].add(value)

        while True:
            negcount = 0
            for qi_sequence, idxset in qi_frequency.items():
                if len(idxset) < k:
                    negcount += len(idxset)
            
            if negcount > k:
                most_freq_att_num, most_freq_att_name = -1, None
                for qiname in qi_names:
                    if len(domains[qiname]) > most_freq_att_num:
                        most_freq_att_num = len(domains[qiname])
                        most_freq_att_name = qiname
                
                # find the attribute with most distinct values
                generalize_att = most_freq_att_name
                qi_index = qi_names.index(generalize_att)
                domains[generalize_att] = set()
                
                # generalize that attribute to one higher level
                for qi_sequence in list(qi_frequency.keys()):
                    new_qi_sequence = list(qi_sequence)
                    new_qi_sequence[qi_index] =  generalize_tree[generalize_att].root[str(qi_sequence[qi_index])][0]
                    new_qi_sequence = tuple(new_qi_sequence)
                
                    if new_qi_sequence in qi_frequency:
                        qi_frequency[new_qi_sequence].update(
                            qi_frequency[qi_sequence])
                        qi_frequency.pop(qi_sequence, 0)
                    else:
                        qi_frequency[new_qi_sequence] = qi_frequency.pop(qi_sequence)
                    
                    domains[generalize_att].add(new_qi_sequence[qi_index])
                
                gen_levels[generalize_att] += 1
                
            
            else:
                genlvl_att = [0 for _ in range(len(qi_names))]
                dgh_att = [generalize_tree[name].level for name in qi_names]
                datasize = 0
                qiindex = [self.ATTNAME.index(name) for name in qi_names]

                # used to make sure the output file keeps the same order with original data file
                towriterecords = [None for _ in range(len(self.records))]
                output = []
                for qi_sequence, recordidxs in qi_frequency.items():
                    if len(recordidxs) < k:
                        continue
                    for idx in recordidxs:
                        record = self.records[idx][:]
                        for i in range(len(qiindex)):
                            record[qiindex[i]] = qi_sequence[i]
                            genlvl_att[i] += generalize_tree[qi_names[i]].root[qi_sequence[i]][1]
                        record = list(map(str, record))
                        for i in range(len(record)):
                            if record[i] == '*' and i not in qiindex:
                                record[i] = '?'
                        towriterecords[idx] = record[:]
                        # wf.write(', '.join(record))
                        # wf.write('\n')
                    datasize += len(recordidxs)
                for record in towriterecords:
                    if record is not None:
                        output.append(record)
                    else:
                        temp = []
                        for a in range(len(self.ATTNAME)):
                            temp.append('')
                        output.append(temp)
                
                #print('qi names: ', qi_names)
                # precision = self.calc_precission(genlvl_att, dgh_att, datasize, len(qi_names))
                precision = self.calc_precision(genlvl_att, dgh_att, len(self.records), len(qi_names))
                distoration = self.calc_distoration([gen_levels[qi_names[i]] for i in range(len(qi_names))], dgh_att, len(qi_names))
                #print('precision: {}, distoration: {}'.format(precision, distoration))
                return output
                break


    def calc_precision(self, genlvl_att, dgh_att, datasize, attsize = 4):
        return 1 - sum([genlvl_att[i] / dgh_att[i] for i in range(attsize)])/(datasize*attsize)


    def calc_distoration(self, gen_levels_att, dgh_att, attsize):
        a = "stub"
        #print('attribute gen level:', gen_levels_att)
        #print('tree height:', dgh_att)
        #return sum([gen_levels_att[i] / dgh_att[i] for i in range(attsize)]) / attsize

        
    def _get_qi_values(self, record, qi_names, generalize_tree):
        qi_index = [self.ATTNAME.index(name) for name in qi_names]
        seq = []
        for idx in qi_index:
            if record[idx] == '*':
                # TODO, handle missing value cases
                record[idx] = generalize_tree[qi_names[idx]].highestgen
            seq.append(record[idx])
        return tuple(seq)

            

        
class Tree:
    def __init__(self, confile):
        self.confile = confile
        self.root = dict()
        self.level = -1
        self.highestgen = ''
        self.buildTree()
        
    
    def buildTree(self):
        with open(self.confile, 'r') as rf:
            for line in rf:
                line = line.strip()
                if not line:
                    continue
                line = [col.strip() for col in line.split(',')]
                height = len(line)-1
                if self.level == -1:
                    self.level = height
                if not self.highestgen:
                    self.highestgen = line[-1]
                pre = None
                for idx, val in enumerate(line[::-1]):
                    self.root[val] = (pre, height-idx)
                    pre = val


class DataConverter:
    @staticmethod
    def ConvertStrings(table):
        for row_idx in range(len(table)):
            for col_idx in range(len(table[row_idx])):
                if table[row_idx][col_idx].strip().isdigit():
                    table[row_idx][col_idx] = str(table[row_idx][col_idx].strip())
                else:
                    table[row_idx][col_idx] = table[row_idx][col_idx].strip()
                if table[row_idx][col_idx] == "?" or table[row_idx][col_idx] == "":
                    table[row_idx][col_idx] = "*"

class CsvReader:
    @staticmethod
    def ReadCsvTable(path, parseHeader = False):
        with open(path) as csvfile:
            table_raw = list(filter(len, csv.reader(csvfile, delimiter=',')))
        DataConverter.ConvertStrings(table_raw)
        if not parseHeader:
            return table_raw
        else:
            return table_raw[0], table_raw[1:]


class DataFlyRunner(Runner):
    def Run(self):
        self.ReadConfig('Datafly')
        input_table_full_path = self.table_path_base + self.table_name
        header, table = CsvReader.ReadCsvTable(input_table_full_path, True)
        datafly = DataFly(table)

        result_raw = datafly.anonymize(header, self.qi_names, self.config["datafly"]["vgh_data"], self.k_value)

        result = [header]
        result.extend(result_raw)

        self.WriteOutput(result)
        return {"in" : input_table_full_path, "out" : self.output_path}

'''
def main():

    runner = DataFlyRunner("C:\\Users\\79313\\Documents\\repos\\k-anon-scratch\\config_main.json")
    runner.Run()

if __name__ == "__main__":
    main()

'''