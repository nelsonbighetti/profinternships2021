import csv

class CsvWriter:
    @staticmethod
    def Write(path, header, rows):
        with open(path, 'w', newline='') as f:
            w = csv.DictWriter(f, header, dialect='excel', delimiter=',', quotechar='"')
            w.writeheader()
            for row in rows:
                w.writerow(row)

    @staticmethod
    def WriteList(path, data):
        header = data[0]
        rows = []

        for row in data[1:]:
            idx = 0
            row_tmp = {}
            for col in row:
                row_tmp[header[idx]] = col
                idx+=1
            rows.append(row_tmp)

        CsvWriter.Write(path, header, rows)
