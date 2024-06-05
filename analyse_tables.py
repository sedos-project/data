import pandas as pd
import os
import pathlib
import glob


def analyse_col_values(table_name: str, df_table: object, value_in_col: dict, value_not_in_col: dict ) -> object:

    for column, col_value in value_in_col.items():
        if column in df_table.columns:
            for item in df_table[column]:
                if col_value in str(item):
                    found_value = item
                    res_value_in_col[f"{table_name}"] = found_value

    for column, col_value in value_not_in_col.items():
        if column in df_table.columns:
            set_wrong_values_in_col = set()
            for item in df_table[column]:
                if pd.notna(item) and col_value not in str(item):
                    set_wrong_values_in_col.add(item)

            # check if set is empty
            if set_wrong_values_in_col:
                res_value_not_in_col[f"{table_name}"] = set_wrong_values_in_col



def return_csv_table_paths(path: pathlib.Path) -> list:
    return glob.glob(f"{path}*.csv")



if __name__ == "__main__":

    res_value_in_col = {}
    res_value_not_in_col = {}

    value_in_col = {"wacc": "economic"}
    value_not_in_col = {"version": "srd_range_draft"}


    path = r"2024-06-04/hea/"

    tables_paths = return_csv_table_paths(path)

    for table_path in tables_paths:
        table_name = table_path.split("\\")[1].split(".")[0]
        print(table_name)
        df_table = pd.read_csv(
                filepath_or_buffer=table_path,
                sep=",")
        analyse_col_values(table_name, df_table, value_in_col, value_not_in_col)


    print("res_value_in_col:",  res_value_in_col)
    print("res_value_not_in_col:",  res_value_not_in_col)


    result_df = pd.DataFrame.from_dict(res_value_not_in_col, orient='index')

    print("df:", result_df)