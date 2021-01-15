import csv

class DataConverter:
    # Конвертация строк в hex, затем - в числа в десятичной системе счисления
    # Если столбец содержит хотя бы одно значение, не являющееся числом - он конвертируется целиком
    # Принимает на вход двумерный массив
    @staticmethod
    def ConvertStringsToHex(table):
        # Хранит номера столбцов, которые были конвертированы
        col_converted = set()
        for row_idx in range(len(table)):
            for col_idx in range(len(table[row_idx])):
                if type(table[row_idx][col_idx]) is int:
                    continue
                if not table[row_idx][col_idx].isdigit():
                    col_converted.add(col_idx)
                else:
                    table[row_idx][col_idx] = int(table[row_idx][col_idx])

        for col_idx in col_converted:
            for row_idx in range(len(table)):
                table[row_idx][col_idx] = int(str(table[row_idx][col_idx]).encode('utf-8').hex(), 16)

        return col_converted


    @staticmethod
    def ConvertHexToStrings(table, col_converted):
        for col_idx in col_converted:
            for row_idx in range(len(table)):
                # Работа с интервалами
                if isinstance(table[row_idx][col_idx], list):
                    hex_str_lo = hex(table[row_idx][col_idx][0])[2:]
                    hex_str_hi = hex(table[row_idx][col_idx][1])[2:]
                    str_lo = bytes.fromhex(hex_str_lo).decode('utf-8')
                    str_hi = bytes.fromhex(hex_str_hi).decode('utf-8')
                    table[row_idx][col_idx] = str_lo + '-' + str_hi
                else:
                    hex_str = hex(table[row_idx][col_idx])[2:]
                    if(len(hex_str)%2):
                        hex_str = hex_str+'0'
                    table[row_idx][col_idx] = bytes.fromhex(hex_str).decode('utf-8')

    @staticmethod
    def ConvertStringsWithIntervalsToHex(table):
        for row_idx in range(len(table)):
            for col_idx in range(len(table[row_idx])):
                if "-" in table[row_idx][col_idx]:
                    splitted = table[row_idx][col_idx].split('-')
                    if not splitted[0].isdigit():
                        splitted[0] = int(str(splitted[0]).encode('utf-8').hex(), 16)
                    else:
                        splitted[0] = int(splitted[0])

                    if not splitted[1].isdigit():
                        splitted[1] = int(str(splitted[1]).encode('utf-8').hex(), 16)
                    else:
                        splitted[1] = int(splitted[1])

                    table[row_idx][col_idx] = [splitted[0], splitted[1]]
                else:
                    if not table[row_idx][col_idx].isdigit():
                        if len(table[row_idx][col_idx]) > 0:
                            table[row_idx][col_idx] = int(str(table[row_idx][col_idx]).encode('utf-8').hex(), 16)
                        else:
                            table[row_idx][col_idx] = 0
                    else:
                        table[row_idx][col_idx] = int(table[row_idx][col_idx])



class CsvReader:
    @staticmethod
    def ReadCsvTable(path, parseHeader = False, convertStr = False):
        with open(path) as csvfile:
            table_raw = list(filter(len, csv.reader(csvfile, delimiter=',')))

        header = list(table_raw[0])

        if convertStr:
            DataConverter.ConvertStringsWithIntervalsToHex(table_raw)
        if not parseHeader:
            return table_raw
        else:
            return header, table_raw[1:]
