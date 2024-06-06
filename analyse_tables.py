'''
Script to analyse SEDOS data tables for formal correctness.
'''

import pandas as pd
import pathlib
import glob

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


class EmptyColumnError(Exception):
    pass


# define sector to analyse
sector = "tra"

# define values to check if/if not in column
value_in_col = {"type": "1", "year": "2020", "year": "2021"}
value_not_in_col = {"version": "srd_range_draft", "source": "1"}

# constants

path = fr"2024-06-04/{sector}/"
excel_path = fr'review/{sector}_review_results.xlsx'
bwshare_path = fr"SEDOS_Modellstruktur.xlsx"

processes_bwshare = pd.read_excel(io=bwshare_path, sheet_name="Process_Set", usecols=["process"])
processes_bwshare = set(processes_bwshare["process"].dropna())

parameter_bwshare = pd.read_excel(io=bwshare_path, sheet_name="Parameter_Set",
                                  usecols=["SEDOS_name_long", "static_parameter"])
nomenclature_static = set(parameter_bwshare.loc[parameter_bwshare["static_parameter"] == 1, "SEDOS_name_long"])
nomenclature_variable = set(parameter_bwshare.loc[parameter_bwshare["static_parameter"] == 0, "SEDOS_name_long"])

OED_COLS = {
    "id",
    "region",
    "year",
    "type",
    "bandwidth_type",
    "version",
    "method",
    "source",
    "comment",
    "timeindex_start",
    "timeindex_stop",
    "timeindex_resolution"
}

global_tables = [f"{sector}_timeseries", f"{sector}_scalars", "global_timeseries", "global_scalars",
                 "global_emission_factors"]

global_emission_cols = [
    "raw_hard_coal_power_stations_industry",
    "hard_coal_briquettes",
    "hard_coal_coke",
    "hard_coal_coke_iron_steel",
    "anthracite_heat_market_residential_tcs",
    "coking_coal",
    "hard_coal_for_the_iron_and_steel_industry",
    "other_hard_coal_products",
    "hard_coal_tar",
    "benzene",
    "raw_lignite_public_district_heating_stations",
    "raw_lignite_small_consumers",
    "raw_lignite_coalfield_public_power_stations_rheinland",
    "raw_lignite_coalfield_public_power_stations_lausitz",
    "raw_lignite_coalfield_public_power_stations_mitteldeutschland",
    "lignite_briquettes",
    "lignite_dust_and_fluidised_bed_coal",
    "lignite_coke",
    "meta_lignite",
    "crude_oil",
    "gasoline",
    "naphtha",
    "kerosene",
    "avgas",
    "diesel_fuel",
    "light_heating_oil",
    "heavy_fuel_oil",
    "petroleum",
    "petroleum_coke_without_catalyst_regeneration",
    "lp_gas_energy_related_consumption",
    "refinery_gas",
    "other_petroleum_products",
    "lubricants",
    "coke_oven_gas",
    "top_gas_and_converter_gas",
    "other_produced_gases",
    "natural_gas",
    "petroleum_gas",
    "pit_gas",
    "household_municipal_waste",
    "industrial_waste",
    "special_waste",
    "used_oil",
    "waste_plastics",
    "waste_tyres",
    "bleaching_clay",
    "sewagesludge_2mj_per_kg",
    "sewagesludge_4mj_per_kg",
    "sewagesludge_6mj_per_kg",
    "sewagesludge_8mj_per_kg",
    "sewagesludge_10mj_per_kg",
    "solvents_waste",
    "spent_liquors_from_pulp_production",
    "fibre_deinking_residues",
    "firewood_untreated",
    "waste_wood_wood_scraps_industry",
    "waste_wood_wood_scraps_commercial_institutional",
    "bark",
    "animal_meals_and_fats",
    "biogas",
    "landfill_gas",
    "sewage_gas",
    "bioethanol",
    "cng",
    "lng",
    "lp_gas_energy_related_consumption",
    "methanol",
    "biodiesel"
]

global_scalar_cols = ["wacc"]
global_timeseries_cols = []  # does not exist yet
sector_timeseries_cols = ["capacity_tra_connection_flex_lcar", "capacity_tra_connection_flex_mcar",
                          "capacity_tra_connection_flex_hcar", "capacity_tra_connection_inflex_lcar",
                          "capacity_tra_connection_inflex_mcar", "capacity_tra_connection_inflex_hcar",
                          "exo_pkm_road_lcar", "exo_pkm_road_mcar", "exo_pkm_road_hcar", "sto_max_lcar",
                          "sto_max_mcar", "sto_max_hcar", "sto_min_lcar", "sto_min_mcar", "sto_min_hcar"]

global_cols = global_emission_cols + global_scalar_cols + global_timeseries_cols


def find_wanted_values_in_col(table_name: str, df_table: pd.DataFrame, value_in_col: dict) -> None:
    '''
    Find missing and incorrect column values in tables and log them.

    Parameters
    ----------
    table_name: str
        Table name
    df_table: pandas.DataFrame
        Data to check
    value_in_col: dict
        Columns and values to check
    value_not_in_col: dict
        Columns and values to check
    Returns
    -------
    None
    '''

    res = pd.DataFrame()
    table_missing_cols = pd.DataFrame()
    res_value_in_col = pd.DataFrame()
    res_value_not_in_col = pd.DataFrame()

    # check if value is in column - log if it is
    for column, col_value in value_in_col.items():
        if column in df_table.columns:
            for item in df_table[column]:
                if col_value in str(item):
                    found_value = item
                    res_value_in_col[f"{table_name}"] = found_value
                    res = pd.concat([res, pd.DataFrame([{'table_name': table_name, 'searched_column': column,
                                                         'value_expected_to_be_in_col': col_value,
                                                         'value_found_in_col': found_value}])], ignore_index=True)
        else:
            res = pd.concat([res, pd.DataFrame([{'table_name': table_name, 'searched_column': column,
                                                 'missing_column': column}])], ignore_index=True)

    return res


def find_unwanted_values_in_col(table_name: str, df_table: pd.DataFrame, res_value_not_in_col: dict):
    '''

    Parameters
    ----------
    table_name
    df_table
    res_value_not_in_col

    Returns
    -------

    '''
    # check if value is missing in column - log if it is
    for column, col_value in value_not_in_col.items():
        if column in df_table.columns:
            set_wrong_values_in_col = set()
            for item in df_table[column]:
                if item not in set_wrong_values_in_col:
                    if pd.notna(item) and col_value not in str(item):
                        set_wrong_values_in_col.add(item)
                        res = pd.concat([res, pd.DataFrame([{'table_name': table_name, 'searched_column': column,
                                                             'value_expected_to_be_in_col': col_value,
                                                             'value_found_in_col': found_value}])], ignore_index=True)

            # check if set is empty
            if set_wrong_values_in_col:
                res_value_not_in_col[f"{table_name}|{column}|{col_value}"] = set_wrong_values_in_col

    return res


def get_user_cols(df_table):
    return [col for col in df_table.columns if col not in OED_COLS]


def check_global_reference_cols(table_name: str, df_table: object) -> None:
    '''
    Find foreign-key table references where the referenced column is missing in reference table.

    Parameters
    ----------
    table_name: str
        Table name
    df_table: pandas.DataFrame
        Data to check

    Returns
    -------
    miss_ref: pandas.DataFrame
        Data with missing columns in reference table
    '''
    miss_ref = pd.DataFrame()
    # extract set of user cols
    user_cols = get_user_cols(df_table)

    # New DataFrame with the selected columns
    user_df = df_table[user_cols]

    # Extract columns that contain one of the search strings and are not in OED_COLS
    for user_col in user_cols:
        for value in user_df[user_col].astype(str).unique():
            if "[" not in value and "." in value:
                global_table_ref, ref_col = value.split(".")

                # Check if ref_col is not in global_cols and is not a number
                if ref_col not in global_cols and not ref_col.isdigit():
                    miss_ref = pd.concat([miss_ref, pd.DataFrame([{'table_name': table_name, 'column': user_col,
                                                                   'global_table_ref': global_table_ref,
                                                                   'ref_col': ref_col}])], ignore_index=True)

    return miss_ref


def check_if_process_name_is_in_set_bwshare_process_names(table_name, df_table):
    '''
    Returns missing processes

    Parameters
    ----------
    table_name: str
        Table name
    df_table: pandas.DataFrame
        Data to check

    Returns
    -------
    missing_processes: pandas.DataFrame
        Dataframe of missing processes
    '''
    missing_processes = pd.DataFrame()
    if "type" not in df_table.columns and table_name not in processes_bwshare:
        missing_processes = pd.concat([missing_processes, pd.DataFrame([{'table_name': table_name}])],
                                      ignore_index=True)
        return missing_processes

    if df_table["type"].isna().all():
        missing_processes = pd.concat([missing_processes, pd.DataFrame([{'table_name': table_name,
                                                                         'type_col_processes_on_oep_but_missing_in_bwshare':
                                                                             "TYPE COLUMN MUST NOT BE EMPTY"}])],
                                      ignore_index=True)

    table_processes = set(df_table["type"].dropna())
    is_subset = table_processes.issubset(processes_bwshare)
    if not is_subset:
        miss_pros = table_processes - processes_bwshare
        missing_processes = pd.concat([missing_processes, pd.DataFrame([{'table_name': table_name,
                                                                         'type_col_processes_on_oep_but_missing_in_bwshare':
                                                                             miss_pros}])], ignore_index=True)
        return missing_processes

    return None


def check_if_column_name_is_in_set_bwshare_parameter_names(table_name, df_table):
    '''
    Returns missing parameters

    Parameters
    ----------
    table_name: str
        Table name
    df_table: pandas.DataFrame
        Data to check
    Returns
    -------
    missing_parameters: pandas.DataFrame
        Dataframe of missing parameters
    '''
    missing_parameters = pd.DataFrame()

    # parameter check
    table_parameters = set(get_user_cols(df_table))

    prefixes_nomenclature_variable = [s.split('<')[0] for s in nomenclature_variable]
    variable_parameters = {item for item in table_parameters if
                           any(item.startswith(prefix) for prefix in prefixes_nomenclature_variable)}
    static_parameters = {item for item in table_parameters if
                         not any(item.startswith(prefix) for prefix in prefixes_nomenclature_variable)}

    is_static = static_parameters.issubset(nomenclature_static)
    is_variable = variable_parameters.issubset(nomenclature_variable)

    static_params = None if is_static else static_parameters - nomenclature_static
    variable_params = None if is_variable else variable_parameters - nomenclature_variable

    missing_parameters = pd.concat([missing_parameters, pd.DataFrame([{'table_name': table_name,
                                                                       'static_column_name_in_table_is_not_in_bwshare_parameter_set':
                                                                           static_params,
                                                                       'variable_column_name_in_table_is_not_in_bwshare_parameter_set | ATTENTION: so far just lists all static parameters and does not check if build logic is correct':
                                                                           variable_params}])], ignore_index=True)


    return missing_parameters


def return_csv_table_paths(path: pathlib.Path) -> list:
    '''

    Parameters
    ----------
    path: str
        Path to files

    Returns
    -------
    List of file paths
    '''
    return glob.glob(f"{path}*.csv")


if __name__ == "__main__":

    df_wrong_col_values = pd.DataFrame()
    df_missing_ref_table_columns = pd.DataFrame()
    df_missing_proc = pd.DataFrame()
    df_wrong_parameter_name = pd.DataFrame()

    tables_paths = return_csv_table_paths(path)

    for table_path in tables_paths:
        table_name = table_path.split("\\")[1].split(".")[0]
        print(table_name)
        df_table = pd.read_csv(
            filepath_or_buffer=table_path,
            sep=",")

        # perform different tests

        #df_wrong_col_values = pd.concat([df_wrong_col_values, find_wanted_values_in_col(table_name, df_table,
        # value_in_col)], ignore_index=True)

        df_missing_ref_table_columns = pd.concat([df_missing_ref_table_columns, check_global_reference_cols(
            table_name, df_table)], ignore_index=True)

        df_missing_proc = pd.concat([df_missing_proc, check_if_process_name_is_in_set_bwshare_process_names(
            table_name, df_table)], ignore_index=True)

        df_wrong_parameter_name = pd.concat(
            [df_wrong_parameter_name, check_if_column_name_is_in_set_bwshare_parameter_names(
                table_name, df_table)], ignore_index=True)

    # save test results to one excel in different sheets

    dfs = {"wrong_values": df_wrong_col_values, "missing_refs": df_missing_ref_table_columns, "missing_processes":
        df_missing_proc, "wrong_parameter_name": df_wrong_parameter_name}

    for title, df in dfs.items():
        with pd.ExcelWriter(excel_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=title, index=False)
