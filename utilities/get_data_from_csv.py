
# Josephat Mwakyusa, August 10, 2022

import csv
class GetDataFromCSVFile:
    def __init__(self, file_path):
        self.file_path = file_path

    async def get_departments(self):
        dpt_data= []
        dpt_file = open(self.file_path)
        csv_arr_object = csv.reader(dpt_file)
        next(csv_arr_object)
        for data_row in csv_arr_object:
            dpt_data.append(data_row)
        print(dpt_data)
        return dpt_data
