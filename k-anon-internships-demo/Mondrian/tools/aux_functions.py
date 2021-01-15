import time
import datetime

def cmp_int(x, y):
    # сравнение числа с числом
    if x > y:
        return 1
    elif x==y:
        return 0
    else:
        return -1

def cmp_str(element1, element2):
    # лексикографическое сравнение
    try:
        return cmp_int(int(element1), int(element2))
    except ValueError:
        return cmp_int(element1, element2)

def cmp_value(element1, element2):
    # сравнение строки со строкой, числа с числом
    if isinstance(element1, str):
        return cmp_str(element1, element2)
    else:
        return cmp_int(element1, element2)

def value(x):
    # Возвращает числовой тип, поддерживающий добавление и вычитание
    if isinstance(x, (int, float)):
        return float(x)
    elif isinstance(x, datetime.datetime):
        return time.mktime(x.timetuple())
    else:
        try:
            return float(x)
        except Exception as e:
            return x