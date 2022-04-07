# Импортируем библиотеку requests
import requests
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import gspread
from gspread_dataframe import set_with_dataframe
from accs import key_json_name, key_api

# Загружаем параметры из json
config_variables = json.load(open('params.json'))
# Устанавливаем переменные
locals().update(config_variables)

# Вычисляем дату начала и дату окончания
DayStart = datetime.today() + relativedelta(years=-1)
DayStart = DayStart.strftime('%Y-%m-%d')
DayEnd = datetime.today().strftime('%Y-%m-%d')

# ACCES GOOGLE SHEET
sa = gspread.service_account(filename=key_json_name)
sh = sa.open_by_key(key_api)
worksheet = sh.worksheet("ya_fp_py")

def get_data_to_excel(access_token, metric_id, file_name):

    # Адрес api метода для запроса get
    url_param = "https://api-metrika.yandex.net/stat/v1/data"

    # параметры выгрузки
    api_params = {
        "ids": metric_id,
        "metrics": "ym:s:users,ym:s:visits,ym:s:pageviews,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds,ym:s:goal228228461users",
        "dimensions": "ym:s:date,ym:s:<attribution>TrafficSource,ym:s:<attribution>SourceEngine",
        "date1": DayStart,
        "date2": DayEnd,
        "sort": "ym:s:date",
        "accuracy": "full",
        "attribution": "lastsign",
        "limit": 100000
    }

    # Задаем параметры header_params
    header_params = {
        'GET': '/management/v1/counters HTTP/1.1',
        'Host': 'api-metrika.yandex.net',
        'Authorization': 'OAuth ' + access_token,
        'Content-Type': 'application/x-yametrika+json'
    }

    # Отправляем get request (запрос GET)
    response = requests.get(
        url_param,
        params=api_params,
        headers=header_params
    )

    # Преобразуем response с помощью json()
    result = response.json()
    result = result['data']

    # Создаем пустой dict (словарь данных)
    dict_data = {}

    # Парсим исходный list формата Json в dictionary (словарь данных)
    for i in range(0, len(result) - 1):
        dict_data[i] = {
            'date': result[i]["dimensions"][0]["name"],
            'traffic-source': result[i]["dimensions"][1]["name"],
            'traffic-details': result[i]["dimensions"][2]["name"],
            'users': result[i]["metrics"][0],
            'visits': result[i]["metrics"][1],
            'pageviews': result[i]["metrics"][2],
            'bounceRate': result[i]["metrics"][3],
            'pageDepth': result[i]["metrics"][4],
            'avgVisitDurationSeconds': result[i]["metrics"][5],
            'leads': result[i]["metrics"][6]
        }

    # Создаем DataFrame из dict (словаря данных или массива данных)
    dict_keys = dict_data[0].keys()
    df = pd.DataFrame.from_dict(dict_data, orient='index', columns=dict_keys)

    # CLEAR SHEET CONTENT
    range_of_cells = worksheet.range('A2:O10000')  # -> Select the range you want to clear
    for cell in range_of_cells:
        cell.value = ''
    worksheet.update_cells(range_of_cells)

    # APPEND DATA TO SHEET
    set_with_dataframe(worksheet, df)  # -> THIS EXPORTS YOUR DATAFRAME TO THE GOOGLE SHEET

    print('данные отправлены в гугл таблицы')
    # worksheet.update('A1:B2', [[1, 2], [3, 4]])

    # Выгрузка данных из DataFrame в Excel
    df.to_excel(file_name, sheet_name='data', index=False)


if __name__ == "__main__":

    for (site_name,metric_id) in METRIC_IDS.items():

        # Название файла Excel
        file_name = site_name + metric_id + "_Трафик.xlsx"

        # ключи для связки
        access_token = ACCESS_TOKEN

        # Вызываем функцию
        get_data_to_excel(access_token, metric_id, file_name)