import pandas as pd
import os
import pathlib
import glob


def analyse_col_values(table_name: str, df_table: object, columns_value_dict: dict ) -> object:

    for column, col_value in columns_value_dict.items():
        if column in df_table.columns:
            for item in df_table[column]:
                if col_value in str(item):
                    found_value = item
                    result_table_dict[f"{table_name}"] = found_value

def return_csv_table_paths(path: pathlib.Path) -> list:
    return glob.glob(f"{path}*.csv")



if __name__ == "__main__":

    result_table_dict = {}
    columns_value_dict = {"wacc": "economic"}


    path = r"AP8/"

    tables_paths = return_csv_table_paths(path)

    for table_path in tables_paths:
        table_name = table_path.split("\\")[1].split(".")[0]
        df_table = pd.read_csv(
                filepath_or_buffer=table_path,
                sep=";")
        analyse_col_values(table_name, df_table, columns_value_dict)

    print(result_table_dict)