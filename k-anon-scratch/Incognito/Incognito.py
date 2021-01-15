from sqlite3 import dbapi2 as sqlite3
from os import path
from sympy import subsets
import argparse
import json
import queue
import csv
from common_tools.AlgsRunner import Runner
from common_tools.CsvReadAndConvert import *
import time

class Incognito:
    connection = None 
    cursor = None
    dataset = None 
    dimension_tables = None 
    dimension_tables_path = None 
    threshold = None 
    attributes = None 
    Q = None

    def run(self, dtset_path, dim_tables_path, kval, thrsh):
        self.connection = None
        self.cursor = None
        self.dataset = None
        self.dimension_tables = None
        self.dimension_tables_path = None
        self.threshold = None
        self.attributes = None
        self.Q = None

        self.connection = sqlite3.connect(":memory:")
        self.cursor = self.connection.cursor()
        self.cursor.execute("PRAGMA synchronous = OFF")
        self.cursor.execute("PRAGMA journal_mode = OFF")
        self.cursor.execute("PRAGMA locking_mode = EXCLUSIVE")

        # all self.attributes of the table
        self.attributes = list()

        self.dataset = dtset_path
        self.dimension_tables_path = dim_tables_path

        self.prepare_table_to_be_k_anonymized()
        self.dataset = path.basename(self.dataset).split(".")[0]

        # get dimension tables
        self.dimension_tables = self.get_dimension_tables()
        self.Q = set(self.dimension_tables.keys())

        # create dimension SQL tables
        self.create_dimension_tables(self.dimension_tables)

        self.k = kval
        self.cursor.execute("SELECT * FROM " + str(self.dataset))
        if self.k > len(list(self.cursor)) or self.k <= 0:
            print("ERROR: self.k value is invalid")
            exit(0)

        try:
            self.threshold = thrsh
            if self.threshold >= self.k or self.threshold < 0:
                print("ERROR: self.threshold value is invalid")
                exit(0)
        except:
            self.threshold = 0
            pass

        # the first domain generalization hierarchies are the simple A0->A1, O0->O1->O2 and, obviously, the first candidate
        # nodes Ci (i=1) are the "0" ones, that is Ci={A0, O0}
        self.create_tables_Ci_Ei()

        # we must pass the priorityQueue otherwise the body of the function can't see and instantiates a PriorityQueue
        self.basic_incognito_algorithm(queue.PriorityQueue())

        self.cursor.execute("SELECT * FROM S" + str(len(self.Q)))
        Sn = list(self.cursor)

        anon_table = self.projection_of_attributes_of_Sn_onto_T_and_dimension_tables(Sn)

        self.connection.close()

        return anon_table


    def prepare_table_to_be_k_anonymized(self):
        with open(self.dataset, "r") as dataset_table:
            table_name = path.basename(self.dataset).split(".")[0]
            #print("Working on self.dataset " + table_name)

            # first line contains attribute names
            attribute_names = dataset_table.readline().split(",")
            for attr in attribute_names:
                table_attribute = attr.strip() + " TEXT"
                self.attributes.append(table_attribute.replace("-", "_"))
            self.cursor.execute("CREATE TABLE IF NOT EXISTS " + table_name + "(" + ','.join(self.attributes) + ")")
            #print("Attributes found: " + str([attr.strip() for attr in attribute_names]))

            # insert records into the SQL table
            for line in dataset_table:
                values = line.split(",")
                new_values = list()
                for value in values:
                    value = value.strip()
                    if value.__contains__("-"):
                        value = value.replace("-", "_")
                    new_values.append(value)

                # a line could be a "\n" => new_values ===== [''] => len(new_values) == 1
                if len(new_values) == 1:
                    continue
                self.cursor.execute("INSERT INTO " + table_name + ' values ({})'.format(new_values)
                               .replace("[", "").replace("]", ""))


    def get_dimension_tables(self):
        #print("Getting dimension tables", end="")
        json_text = ""
        with open(self.dimension_tables_path, "r") as dimension_tables_filename:
            for line in dimension_tables_filename:
                json_text += line.strip()
        #print("\t OK")
        return json.loads(json_text)


    def create_dimension_tables(self, tables):
        for qi in tables:

            # create SQL table
            columns = list()
            for i in tables[qi]:
                columns.append("'" + i + "' TEXT")
            self.cursor.execute("CREATE TABLE IF NOT EXISTS " + qi + "_dim (" + ", ".join(columns) + ")")

            # insert values into the newly created table
            rows = list()
            for i in range(len(tables[qi]["1"])):
                row = "("
                for j in tables[qi]:
                    row += "'" + str(tables[qi][j][i]) + "', "
                row = row[:-2] + ")"
                rows.append(row)
            self.cursor.execute("INSERT INTO " + qi + "_dim VALUES " + ", ".join(rows))


    def get_parent_index_C1(self, index, parent1_or_parent2):
        parent_index = index - parent1_or_parent2
        if parent_index < 0:
            parent_index = "null"
        return parent_index


    def init_C1_and_E1(self):
        #print("Generating graph for 1 quasi-identifier", end="")
        id = 1
        for dimension in self.dimension_tables:
            index = 0
            for i in range(len(self.dimension_tables[dimension])):
                # parenty = index - y
                # parent2 is the parent of parent1
                parent1 = self.get_parent_index_C1(index, 1)
                parent2 = self.get_parent_index_C1(index, 2)
                tuple = (id, dimension, index, parent1, parent2)
                self.cursor.execute("INSERT INTO C1 values (?, ?, ?, ?, ?)", tuple)
                if index >= 1:
                    self.cursor.execute("INSERT INTO E1 values (?, ?)", (id - 1, id))
                id += 1
                index += 1
        #print("\t OK")


    def create_tables_Ci_Ei(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS C1 (ID INTEGER PRIMARY KEY, dim1 TEXT,"
                       " index1 INT, parent1 INT, parent2 INT)")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS E1 (start INT, end INT)")


    def get_height_of_node(self, node):
        # sum of the indexes in a row (node)
        i = 0
        height = 0
        length = len(node)
        while True:
            # 2, 6, 8, ...
            if i == 0:
                j = 2
            else:
                j = 6 + 2*(i-1)
            if j >= length:
                break
            if node[j] != 'null':
                height += node[j]
            i += 1
        return height


    def get_dimensions_of_node(self, node):
        dimensions_temp = set()
        i = 0
        length = len(node)
        while True:
            # 1, 5, 7, 9, ...
            if i == 0:
                j = 1
            else:
                j = 5 + 2*(i-1)
            if j >= length:
                break
            if node[j] != 'null':
                dimensions_temp.add(node[j])
            i += 1
        return dimensions_temp


    def frequency_set_of_T_wrt_attributes_of_node_using_T(self, node):
        attributes = self.get_dimensions_of_node(node)
        try:
            attributes.remove("null")
        except:
            pass
        dims_and_indexes_s_node = self.get_dims_and_indexes_of_node(node)
        group_by_attributes = set(attributes)
        dimension_table_names = list()
        where_items = list()
        for i in range(len(dims_and_indexes_s_node)):
            if dims_and_indexes_s_node[i][0] == "null" or dims_and_indexes_s_node[i][1] == "null":
                continue
            column_name, dimension_table, dimension_with_previous_generalization_level, generalization_level_str = \
                self.prepare_query_parameters(attributes, dims_and_indexes_s_node, group_by_attributes, i)

            where_item = "" + self.dataset + "." + column_name + " = " + dimension_with_previous_generalization_level

            dimension_table_names.append(dimension_table)
            where_items.append(where_item)

        self.cursor.execute("SELECT COUNT(*) FROM " + self.dataset + ", " + ', '.join(dimension_table_names) +
                       " WHERE " + 'and '.join(where_items) + " GROUP BY " + ', '.join(group_by_attributes))
        freq_set = list()
        for count in list(self.cursor):
            freq_set.append(count[0])
        return freq_set


    def get_dims_and_indexes_of_node(self, node):
        list_temp = list()
        i = 0
        length = len(node)
        while True:
            # dims = 1, 5, 7, ...
            # indexes = 2, 6, 8, ... = dims + 1
            if i == 0:
                j = 1
            else:
                j = 5 + 2*(i-1)
            if j >= length or j+1 >= length:
                break
            list_temp.append((node[j], node[j+1]))
            i += 1
        return list_temp


    def frequency_set_of_T_wrt_attributes_of_node_using_parent_s_frequency_set(self, node, i):
        i_str = str(i)
        dims_and_indexes_s_node = self.get_dims_and_indexes_of_node(node)

        attributes = self.get_dimensions_of_node(node)
        try:
            while True:
                attributes.remove("null")
        except:
            pass
        self.cursor.execute("CREATE TEMPORARY TABLE TempTable (count INT, " + ', '.join(attributes) + ")")

        select_items = list()
        where_items = list()
        group_by_attributes = set(attributes)
        dimension_table_names = list()

        for i in range(len(dims_and_indexes_s_node)):

            if dims_and_indexes_s_node[i][0] == "null" or dims_and_indexes_s_node[i][1] == "null":
                continue
            column_name, dimension_table, dimension_with_previous_generalization_level, generalization_level_str = \
                self.prepare_query_parameters(attributes, dims_and_indexes_s_node, group_by_attributes, i)

            select_item = dimension_table + ".\"" + generalization_level_str + "\" AS " + column_name
            where_item = "" + self.dataset + "." + column_name + " = " + dimension_with_previous_generalization_level

            select_items.append(select_item)
            where_items.append(where_item)
            dimension_table_names.append(dimension_table)

        self.cursor.execute("INSERT INTO TempTable"
                       " SELECT COUNT(*) AS count, " + ', '.join(select_items) +
                       " FROM " + self.dataset + ", " + ', '.join(dimension_table_names) +
                       " WHERE " + 'AND '.join(where_items) +
                       " GROUP BY " + ', '.join(group_by_attributes))

        self.cursor.execute("SELECT SUM(count) FROM TempTable GROUP BY " + ', '.join(attributes))
        results = list(self.cursor)
        freq_set = list()
        for result in results:
            freq_set.append(result[0])

        self.cursor.execute("DROP TABLE TempTable")

        return freq_set


    def prepare_query_parameters(self, attributes, dims_and_indexes_s_node, group_by_attributes, i):
        column_name = dims_and_indexes_s_node[i][0]
        generalization_level = dims_and_indexes_s_node[i][1]
        generalization_level_str = str(generalization_level)
        previous_generalization_level_str = "0"
        dimension_table = column_name + "_dim"
        dimension_with_previous_generalization_level = dimension_table + ".\"" + previous_generalization_level_str + "\""
        if column_name in attributes:
            group_by_attributes.remove(column_name)
            group_by_attributes.add(dimension_table + ".\"" + generalization_level_str + "\"")
        return column_name, dimension_table, dimension_with_previous_generalization_level, generalization_level_str


    def mark_all_direct_generalizations_of_node(self, marked_nodes, node, i):
        i_str = str(i)
        marked_nodes.add(node[0])
        self.cursor.execute("SELECT E" + i_str + ".end FROM C" + i_str + ", E" + i_str + " WHERE ID = E" + i_str +
                       ".start and ID = " + str(node[0]))
        for node_to_mark in list(self.cursor):
            marked_nodes.add(node_to_mark[0])


    def insert_direct_generalization_of_node_in_queue(self, node, queue, i, Si):
        i_str = str(i)
        self.cursor.execute("SELECT E" + i_str + ".end FROM C" + i_str + ", E" + i_str + " WHERE ID = E" + i_str +
                       ".start and ID = " + str(node[0]))
        nodes_to_put = set(self.cursor)

        Si_indices = set()
        for node in Si:
            Si_indices.add(node[0])

        for node_to_put in nodes_to_put:
            # node_to_put == (ID,) -.-
            if node_to_put[0] not in Si_indices:
                continue
            node_to_put = node_to_put[0]
            self.cursor.execute("SELECT * FROM C" + i_str + " WHERE ID = " + str(node_to_put))
            node = (list(self.cursor)[0])
            queue.put_nowait((-self.get_height_of_node(node), node))


    def graph_generation(self, Si, i):
        i_here = i+1
        i_str = str(i)
        ipp_str = str(i+1)
        #if i < len(self.Q):
            #print("Generating graphs for " + ipp_str + " quasi-identifiers", end="")
        # to create Si we need all column names of Ci
        # PRAGMA returns infos like (0, 'ID', 'INTEGER', 0, None, 1), (1, 'dim1', 'TEXT', 0, None, 0), ...
        self.cursor.execute("PRAGMA table_info(C" + i_str + ")")
        column_infos = list()
        column_infos_from_db = list(self.cursor)
        for column in column_infos_from_db:
            if column[1] == "ID":
                column_infos.append("ID INTEGER PRIMARY KEY")
            else:
                column_infos.append(str(column[1]) + " " + str(column[2]))
        self.cursor.execute("CREATE TABLE IF NOT EXISTS S" + i_str + " (" + ', '.join(column_infos) + ")")
        question_marks = ""
        for j in range(0, len(column_infos_from_db) - 1):
            question_marks += " ?,"
        question_marks += " ? "

        self.cursor.executemany("INSERT INTO S" + i_str + " values (" + question_marks + ")", Si)

        self.cursor.execute("SELECT * FROM S" + i_str)
        Si_new = set(self.cursor)

        # in the last iteration the phases are useless because after self.graph_generation only Si (Sn) is taken into account
        if i == len(self.Q):
            return
        i_here_str = str(i_here)
        self.cursor.execute("CREATE TABLE IF NOT EXISTS C" + ipp_str + " (" + ', '.join(column_infos) + ")")
        self.cursor.execute("ALTER TABLE C" + ipp_str + " ADD COLUMN dim" + i_here_str + " TEXT")
        self.cursor.execute("ALTER TABLE C" + ipp_str + " ADD COLUMN index" + i_here_str + " INT")

        self.cursor.execute("UPDATE C" + ipp_str + " SET dim" + i_here_str + " = 'null', index" + i_here_str +
                       "= 'null' WHERE index" + i_here_str + " is null")
        select_str = ""
        select_str_except = ""
        where_str = ""
        for j in range(2, i_here):
            j_str = str(j)
            if j == i_here-1:
                select_str += ", p.dim" + j_str + ", p.index" + j_str + ", q.dim" + j_str + ", q.index" + j_str
                select_str_except += ", q.dim" + j_str + ", q.index" + j_str + ", p.dim" + j_str + ", p.index" + j_str
                where_str += " and p.dim" + j_str + "<q.dim" + j_str
            else:
                select_str += ", p.dim" + j_str + ", p.index" + j_str
                select_str_except += ", q.dim" + j_str + ", q.index" + j_str
                where_str += " and p.dim" + j_str + "=q.dim" + j_str + " and p.index" + j_str + "=q.index" + j_str

        # join phase
        if i > 1:
            self.cursor.execute("INSERT INTO C" + ipp_str +
                            " SELECT null, p.dim1, p.index1, p.ID, q.ID" + select_str +
                            " FROM S" + i_str + " p, S" + i_str + " q WHERE p.dim1 = q.dim1 and p.index1 = q.index1 " + where_str)

        else:
            self.cursor.execute("INSERT INTO C" + ipp_str + " SELECT null, p.dim1, p.index1, p.ID, q.ID, q.dim1, q.index1"
                           " FROM S" + i_str + " p, S" + i_str + " q WHERE p.dim1<q.dim1")

        self.cursor.execute("SELECT * FROM C" + ipp_str)
        Ci_new = set(self.cursor)

        # prune phase
        # all subsets of Si == dims_and_indexes of all nodes in Si
        # for all nodes in Ci+1 we will remove the nodes that contain a subset of dims_and_indexes
        # which is not in all_subsets_of_Si
        all_subsets_of_Si = set()
        for node in Si_new:
            all_subsets_of_Si.add(tuple(self.get_dims_and_indexes_of_node(node)))
        for c in Ci_new:
            for s in subsets(self.get_dims_and_indexes_of_node(c), i):
                if s not in all_subsets_of_Si:
                    node_id = str(c[0])
                    self.cursor.execute("DELETE FROM C" + ipp_str + " WHERE C" + ipp_str + ".ID = " + node_id)

        # edge generation
        self.cursor.execute("CREATE TABLE IF NOT EXISTS E" + ipp_str + " (start INT, end INT)")
        self.cursor.execute("INSERT INTO E" + ipp_str + " "
                       "WITH CandidatesEdges(start, end) AS ("
                       "SELECT p.ID, q.ID "
                       "FROM C" + ipp_str + " p,C" + ipp_str + " q,E" + i_str + " e,E" + i_str + " f "
                       "WHERE (e.start = p.parent1 AND e.end = q.parent1 "
                       "AND f.start = p.parent2 AND f.end = q.parent2) "
                       "OR (e.start = p.parent1 AND e.end = q.parent1 "
                       "AND p.parent2 = q.parent2) "
                       "OR (e.start = p.parent2 AND e.end = q.parent2 "
                       "AND p.parent1 = q.parent1) "
                       ") "
                       "SELECT D.start, D.end "
                       "FROM CandidatesEdges D "
                       "EXCEPT "
                       "SELECT D1.start, D2.end "
                       "FROM CandidatesEdges D1, CandidatesEdges D2 "
                       "WHERE D1.end = D2.start")
        #print("\t OK")

    def table_is_k_anonymous_wrt_attributes_of_node(self, frequency_set):
        if len(frequency_set) == 0:
            return False
        for count in frequency_set:
            if type(count) == tuple:
                count = count[0]
            if self.k > count > self.threshold:
                return False
        return True


    def basic_incognito_algorithm(self, priority_queue):
        self.init_C1_and_E1()
        queue = priority_queue

        marked_nodes = set()

        for i in range(1, len(self.Q) + 1):
            i_str = str(i)
            self.cursor.execute("SELECT * FROM C" + i_str + "")
            Si = set(self.cursor)

            # no edge directed to a node => root
            self.cursor.execute("SELECT C" + i_str + ".* FROM C" + i_str + ", E" + i_str + " WHERE C" + i_str + ".ID = E" +
                           i_str + ".start EXCEPT SELECT C" + i_str + ".* FROM C" + i_str + ", E" + i_str + " WHERE C" +
                           i_str + ".ID = E" + i_str + ".end ")
            roots = set(self.cursor)
            roots_in_queue = set()

            for node in roots:
                height = self.get_height_of_node(node)
                # -height because priority queue shows the lowest first. Syntax: (priority number, data)
                roots_in_queue.add((-height, node))

            for upgraded_node in roots_in_queue:
                queue.put_nowait(upgraded_node)

            while not queue.empty():
                upgraded_node = queue.get_nowait()
                # [1] => pick 'node' in (-height, node);
                node = upgraded_node[1]
                if node[0] not in marked_nodes:
                    if node in roots:
                        frequency_set = self.frequency_set_of_T_wrt_attributes_of_node_using_T(node)
                    else:
                        frequency_set = self.frequency_set_of_T_wrt_attributes_of_node_using_parent_s_frequency_set(node, i)
                    if self.table_is_k_anonymous_wrt_attributes_of_node(frequency_set):
                        self.mark_all_direct_generalizations_of_node(marked_nodes, node, i)
                    else:
                        Si.remove(node)
                        self.insert_direct_generalization_of_node_in_queue(node, queue, i, Si)
                        self.cursor.execute("DELETE FROM C" + str(i) + " WHERE ID = " + str(node[0]))

            self.graph_generation(Si, i)
            marked_nodes = set()



    def projection_of_attributes_of_Sn_onto_T_and_dimension_tables(self, Sn):
        # get node with lowest height, as it should be the least "generalized" one that makes the table self.k-anonymous
        lowest_node = min(Sn, key=lambda t: t[0])
        height = self.get_height_of_node(lowest_node)
        for node in Sn:
            temp_height = self.get_height_of_node(node)
            if temp_height < height:
                height = temp_height
                lowest_node = node

        #print("Chosen anonymization levels: ", end="")

        # get QI names and their indexes (i.e. their generalization level)
        qis = list()
        qi_indexes = list()
        for i in range(len(lowest_node)):
            if lowest_node[i] in self.Q:
                qis.append(lowest_node[i])
                qi_indexes.append(lowest_node[i+1])
                #print(str(lowest_node[i]) + "(" + str(lowest_node[i+1]) + ") ", end="")
        #print("")

        # get all table self.attributes with generalized QI's in place of the original ones
        gen_attr = self.attributes
        considered_gen_qis =list()
        for i in range(len(gen_attr)):
            gen_attr[i] = gen_attr[i].split()[0]
            if gen_attr[i] in qis:
                gen_attr[i] = qis[qis.index(gen_attr[i])] + "_dim.'" + str(qi_indexes[qis.index(gen_attr[i])]) + "'"
                considered_gen_qis.append(gen_attr[i])

        # get dimension tables names
        dim_tables = list()
        for qi in qis:
            dim_tables.append(qi + "_dim")

        # get pairings for the SQL JOIN
        pairs = list()
        for x, y in zip(qis, dim_tables):
            pairs.append(x + "=" + y + ".'0'")


        self.connection.commit()
        reqMain = "SELECT " + ', '.join(gen_attr) + " FROM " + self.dataset + ", " + ', '.join(dim_tables) + \
              " WHERE " + 'AND '.join(pairs)

        subreqs = []

        for needle in considered_gen_qis:
            req = " AND (" + needle + ") IN"\
                " (SELECT " + needle + " FROM " + self.dataset + ", " + ', '.join(dim_tables) +\
                " WHERE " + 'AND '.join(pairs) + "GROUP BY " + needle + " HAVING"\
                " COUNT(*) > " + str(self.threshold) + ")"
            subreqs.append(req)
            reqMain = reqMain + req

        self.cursor.execute(reqMain)

        table = []
        for row in list(self.cursor):
            table.append([x for x in row])

        return table


class IncognitoRunner(Runner):
    def Run(self):
        self.ReadConfig('Incognito')
        input_table_full_path = self.table_path_base + self.table_name
        table_header = CsvReader.ReadCsvTable(input_table_full_path)[0]

        incognito = Incognito()

        result_raw = incognito.run(input_table_full_path,
                         self.config["incognito"]["qi_data"],
                         self.k_value, 0)

        result = [table_header]
        result.extend(result_raw)

        self.WriteOutput(result)
        return  {"in" : input_table_full_path, "out" : self.output_path}

'''
def main():
    start = time.time()
    inRunner = IncognitoRunner("C:\\Users\\79313\\Documents\\repos\\k-anon-scratch\\configs\\generated\\config_whispering_atmosphere_100_0911004705_3k_2q.json")
    inRunner.Run()
    end = time.time()
    print("Total : ", end - start)

if __name__ == "__main__":
    main()
'''