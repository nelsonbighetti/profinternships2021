from Mondrian.tools import aux_functions
from functools import cmp_to_key
import csv
from common_tools.AlgsRunner import Runner
from common_tools.CsvReadAndConvert import *

class Mondrian:
    HEADER = []
    QID = []
    NON_QID_DATA = {}
    QID_POS = []

    DATA = None
    K_VALUE = None
    RESULT = []
    QI_LEN = 0
    QI_DICT = []
    QI_RANGE = []
    QI_ORDER = []
    COL_CONVERTED = []

    class Partition:
        # Используются индексы в списке уникальных значений, вместо непосредственно значений
        def __init__(self, QI_LEN, data, low, high):

            self.low = list(low)  # Нижняя граница
            self.high = list(high)  # Верхняя граница
            self.member = data[:]  # Записи в разделе
            self.allow = [1] * QI_LEN  # Флаг, показывающий, есть ли возможность произвести разбиение

        def add_record(self, record, dim):
            # Добавить одну запись в раздел
            self.member.append(record)

        def add_multiple_record(self, records, dim):
            # Добавить несколько записей в раздел
            for record in records:
                self.add_record(record, dim)

        def __len__(self):
            # Вернуть количество записей в разделе
            return len(self.member)

    def __init__(self, data_raw, QID, k):
        self.HEADER = []
        self.QID = []
        self.NON_QID_DATA = {}
        self.QID_POS = []

        self.DATA = None
        self.K_VALUE = None
        self.RESULT = []
        self.QI_LEN = 0
        self.QI_DICT = []
        self.QI_RANGE = []
        self.QI_ORDER = []
        self.COL_CONVERTED = []

        self.K_VALUE = k
        self.DATA = data_raw
        self.QID = QID
        # Подготовка данных - конвертация строк в числа, удаление столбцов, не находящихся в QID
        self.PrepareData()
        self.QI_LEN = len(QID)

        att_values = []
        # Иниицализация списка уникальных значений атрибутов и списка QID
        for i in range(self.QI_LEN):
            att_values.append(set())
            self.QI_DICT.append(dict())

        # Заполнение списка уникальных значений атрибутов для каждого атрибута из QID
        for record in self.DATA:
            for i in range(self.QI_LEN):
                att_values[i].add(record[i])

        for i in range(self.QI_LEN):
            # Сортировка списка
            value_list = list(att_values[i])
            value_list.sort(key=cmp_to_key(aux_functions.cmp_value))

            # Вычисление диапазона атрибута
            self.QI_RANGE.append(aux_functions.value(value_list[-1]) - aux_functions.value(value_list[0]))

            # Добавление отсортированного списка уникальных значений атрибута в список порядка атрибутов
            self.QI_ORDER.append(list(value_list))

            # Создание словаря, хранящего порядковый номер элемента в отсортированном списке для каждого атрибута
            # в списке квази-идентификаторов
            for index, qi_value in enumerate(value_list):
                self.QI_DICT[i][qi_value] = index

    def get_normalized_width(self, partition, index):
        # Нахождение нормализованного диапазона значений
        d_order = self.QI_ORDER[index]
        width = aux_functions.value(d_order[partition.high[index]]) - aux_functions.value(d_order[partition.low[index]])
        if width == self.QI_RANGE[index]:
            return 1
        return width * 1.0 / self.QI_RANGE[index]

    def choose_dimension(self, partition):
        # Выбор атрибута с наибольшим нормализованным диапазоном
        max_width = -1
        max_dim = -1
        for dim in range(self.QI_LEN):
            if partition.allow[dim] == 0:
                continue
            norm_width = self.get_normalized_width(partition, dim)
            if norm_width > max_width:
                max_width = norm_width
                max_dim = dim
        if max_width > 1:
            raise Exception('max_width : '.format(max_width))
        return max_dim

    def frequency_set(self, partition, dim):
        # Поиск частот для значений атрибута
        frequency = {}
        for record in partition.member:
            try:
                frequency[record[dim]] += 1
            except KeyError:
                frequency[record[dim]] = 1
        return frequency

    def find_median(self, partition, dim):
        # Поиск медианы разбиения с использованием набора частот
        frequency = self.frequency_set(partition, dim)
        split_val = ''
        value_list = list(frequency.keys())
        value_list.sort(key=cmp_to_key(aux_functions.cmp_value))
        total = sum(frequency.values())
        middle = total // 2
        if middle < self.K_VALUE or len(value_list) <= 1:
            try:
                return '', '', value_list[0], value_list[-1]
            except IndexError:
                return '', '', '', ''
        index = 0
        split_index = 0
        for i, qi_value in enumerate(value_list):
            index += frequency[qi_value]
            if index >= middle:
                split_val = qi_value
                split_index = i
                break
        else:
            print("Error: cannot find split_val")
        try:
            next_val = value_list[split_index + 1]
        except IndexError:
            # there is a frequency value in partition
            # which can be handle by mid_set
            # e.g.[1, 2, 3, 4, 4, 4, 4]
            next_val = split_val
        return (split_val, next_val, value_list[0], value_list[-1])

    def removeNonQID(self):
        # Добавление в последний столбец (не подлежащий преобразованию) номера исходной строки, для
        # восстановления значений атрибутов, не входящих в QID
        for row in range(len(self.DATA)):
            self.NON_QID_DATA[row] = {k: v for k, v in enumerate(self.DATA[row])}

        self.DATA = []
        for row in self.NON_QID_DATA:
            temp = []
            for col in self.QID_POS:
                temp.append(str(self.NON_QID_DATA[row][col]))
                del self.NON_QID_DATA[row][col]
            temp.append(str(row))
            self.DATA.append(temp)

    def restoreNonQID(self, output):
        merged = []
        for row in output:
            data_idx = 0
            temp = []
            for col in range(len(self.DATA[0]) - 1 + len(self.NON_QID_DATA[0])):
                if col in self.QID_POS:
                    temp.append(row[data_idx])
                    data_idx += 1
                else:
                    temp.append(self.NON_QID_DATA[row[-1]][col])
            merged.append(temp)
        return merged

    def PrepareData(self):
        self.HEADER = self.DATA[0]
        self.DATA = self.DATA[1:]

        self.QID_POS = [i for i, x in enumerate(self.HEADER) if x in self.QID]

        self.removeNonQID()
        self.COL_CONVERTED = DataConverter.ConvertStringsToHex(self.DATA)


    def anonymize(self, partition):
        # Получение информации о возможности разбиения
        allow_count = sum(partition.allow)
        if allow_count == 0:
            self.RESULT.append(partition)
            return

        for index in range(allow_count):
            # Выбор измерения, по которому будет производиться разбиение
            dim = self.choose_dimension(partition)
            if dim == -1:
                raise Exception('Error: dim=-1')

            # Нахождение медианы для выбранного разбиения и измерения
            (split_val, next_val, low, high) = self.find_median(partition, dim)

            # Обновление нижней и верхней границы
            if low != '':
                partition.low[dim] = self.QI_DICT[dim][low]
                partition.high[dim] = self.QI_DICT[dim][high]

            if split_val == '' or split_val == next_val:
                # Невозможно произвести разделение
                partition.allow[dim] = 0
                continue

            # Разбиение по медиане
            mean = self.QI_DICT[dim][split_val]
            lhs_high = partition.high[:]
            rhs_low = partition.low[:]
            lhs_high[dim] = mean
            rhs_low[dim] = self.QI_DICT[dim][next_val]
            lhs = self.Partition(self.QI_LEN, [], partition.low, lhs_high)
            rhs = self.Partition(self.QI_LEN, [], rhs_low, partition.high)

            for record in partition.member:
                pos = self.QI_DICT[dim][record[dim]]
                if pos <= mean:
                    # lhs = [low, mean]
                    lhs.add_record(record, dim)
                else:
                    # rhs = (mean, high]
                    rhs.add_record(record, dim)
            # Проверка, удовлетворяют lhs и rhs условиям k-анонимности
            if len(lhs) < self.K_VALUE or len(rhs) < self.K_VALUE:
                partition.allow[dim] = 0
                continue

            # Анонимизация подразделов
            self.anonymize(lhs)
            self.anonymize(rhs)
            return

        self.RESULT.append(partition)

    def create_intervals(self, x_left, x_right):
        # Получение результирующих интервалов из граничных значений
        if x_left == x_right:
            result = x_left
        else:
            result = [int(str(x_left)), int(str(x_right))]
        return result

    def merge_intervals(self, table):
        for row_idx in range(len(table)):
            for col_idx in range(len(table[row_idx])):
                if isinstance(table[row_idx][col_idx], list):
                    table[row_idx][col_idx] = str(table[row_idx][col_idx][0]) + '-' + str(table[row_idx][col_idx][1])

    def Run(self):
        low = [0] * self.QI_LEN
        high = [(len(t) - 1) for t in self.QI_ORDER]
        initial_partition = self.Partition(self.QI_LEN, self.DATA, low, high)
        self.anonymize(initial_partition)

        output = []
        for partition in self.RESULT:
            for record in partition.member[:]:
                for index in range(self.QI_LEN):
                    record[index] = self.create_intervals(self.QI_ORDER[index][partition.low[index]],
                                                          self.QI_ORDER[index][partition.high[index]])
                output.append(record)

        DataConverter.ConvertHexToStrings(output, self.COL_CONVERTED)
        self.merge_intervals(output)
        output = self.restoreNonQID(output)
        output.insert(0, self.HEADER)
        return output



class MondrianRunner(Runner):

    def Run(self):
        self.ReadConfig('Mondrian')
        input_table_full_path = self.table_path_base + '\\' + self.table_name
        table = CsvReader.ReadCsvTable(input_table_full_path)
        mondrian = Mondrian(table, self.qi_names, self.k_value)
        result = mondrian.Run()
        self.WriteOutput(result)
        return  {"in" : input_table_full_path, "out" : self.table_name + '_anon'}

'''
def main():
    runner = MondrianRunner("C:\\Users\\79313\\Documents\\repos\\k-anon-scratch\\config_main.json")
    runner.Run()


if __name__ == '__main__':
    main()
'''