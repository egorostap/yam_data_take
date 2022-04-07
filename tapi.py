# import csv

from tapi_yandex_metrika import YandexMetrikaStats
# import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import gspread
from gspread_dataframe import set_with_dataframe
from accs import key_json_name, key_api



ACCESS_TOKEN = "..."
METRIC_IDS = "..."

# Вычисляем дату начала и дату окончания
DayStart = datetime.today() + relativedelta(years=-1)
DayStart = DayStart.strftime('%Y-%m-%d')
DayEnd = datetime.today().strftime('%Y-%m-%d')


# По умолчанию возвращаются только 10000 строк отчета,
# если не указать другое кол-во в параметре limit.
# В отчете может быть больше строк, чем указано в limit
# Тогда необходимо сделать несколько запросов для получения всего отчета.
# Чтоб сделать это автоматически вы можете указать
# параметр receive_all_data=True при инициализации класса.

#Параметры запроса для библиотеки tapi_yandex_metrika
api = YandexMetrikaStats(
    access_token=ACCESS_TOKEN,
    # Если True, будет скачивать все части отчета. По умолчанию False.
    receive_all_data=True
)

# ACCES GOOGLE SHEET
sa = gspread.service_account(filename=key_json_name)
sh = sa.open_by_key(key_api)
worksheet = sh.worksheet("ya_fp_py")

#Параметры запроса для библиотеки tapi_yandex_metrika
params = dict(
    ids = METRIC_IDS,
    metrics = "ym:s:users,ym:s:visits,ym:s:pageviews,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds,ym:s:goal228228461users",
    dimensions = "ym:s:date,ym:s:<attribution>TrafficSource,ym:s:<attribution>SourceEngine,ym:s:gender",
    date1 = DayStart,
    date2 = DayEnd,
    sort = "ym:s:date",
    accuracy="full",
    attribution = "lastsign",
    limit = 10000
)
#Получаем данные из Yandex.Metrika API
result = api.stats().get(params=params)
result = result().data
result = result[0]['data']

#Создаем пустой dict (словать данных)
dict_data = {}

#Парсим исходный list формата Json в dictionary (словарь данных)
for i in range(0, len(result)-1):
    dict_data[i] = {
            'date':result[i]["dimensions"][0]["name"],
            'traffic-source':result[i]["dimensions"][1]["name"],
            'traffic-details':result[i]["dimensions"][2]["name"],
            'users':result[i]["metrics"][0],
            'visits':result[i]["metrics"][1],
            'pageviews':result[i]["metrics"][2],
            'bounceRate':result[i]["metrics"][3],
            'pageDepth':result[i]["metrics"][4],
            'avgVisitDurationSeconds':result[i]["metrics"][5],
            'leads': result[i]["metrics"][6],
          }

#Создаем DataFrame из dict (словаря данных или массива данных)
dict_keys = dict_data[0].keys()
df = pd.DataFrame.from_dict(dict_data, orient='index',columns=dict_keys)


# CLEAR SHEET CONTENT
range_of_cells = worksheet.range('A2:10000') #-> Select the range you want to clear
for cell in range_of_cells:
    cell.value = ''
worksheet.update_cells(range_of_cells)

# APPEND DATA TO SHEET
set_with_dataframe(worksheet, df) #-> THIS EXPORTS YOUR DATAFRAME TO THE GOOGLE SHEET

print('данные отправлены в гугл таблицы')
# worksheet.update('A1:B2', [[1, 2], [3, 4]])

# # Выгрузка данных из DataFrame в Excel
# df.to_excel("Трафик.xlsx",
#         sheet_name='data',
#         index=False)
