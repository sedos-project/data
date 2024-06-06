'''
Script to analyse SEDOS data tables for formal correctness.
'''

import pandas as pd
import pathlib
import glob

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


# define sector to analyse
sector = "x2x"

# define values to check if/if not in column
value_in_col = {"year": "2020"}
value_not_in_col = {"version": "srd_range_draft"}

res_value_in_col = {}
res_value_not_in_col = {}
table_missing_cols = {}
missing_references = {}

# constants

path = fr"2024-06-04/{sector}/"

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
global_timeseries_cols = [] # does not exist yet
sector_timeseries_cols = ["capacity_tra_connection_flex_lcar","capacity_tra_connection_flex_mcar",
                     "capacity_tra_connection_flex_hcar","capacity_tra_connection_inflex_lcar","capacity_tra_connection_inflex_mcar","capacity_tra_connection_inflex_hcar","exo_pkm_road_lcar","exo_pkm_road_mcar","exo_pkm_road_hcar","sto_max_lcar","sto_max_mcar","sto_max_hcar","sto_min_lcar","sto_min_mcar","sto_min_hcar"]

global_cols = global_emission_cols + global_scalar_cols + global_timeseries_cols



def analyse_col_values(table_name: str, df_table: pd.DataFrame, value_in_col: dict, value_not_in_col: dict ) -> None:
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
    # check if value is in column - log if it is
    for column, col_value in value_in_col.items():
        missing_col_set = set()
        if column in df_table.columns:
            for item in df_table[column]:
                if col_value in str(item):
                    found_value = item
                    res_value_in_col[f"{table_name}"] = found_value
        else:
            missing_col_set.add(column)
            if missing_col_set:
                table_missing_cols[table_name] = missing_col_set

    # check if value is missing in column - log if it is
    for column, col_value in value_not_in_col.items():
        if column in df_table.columns:
            set_wrong_values_in_col = set()
            for item in df_table[column]:
                if pd.notna(item) and col_value not in str(item):
                    set_wrong_values_in_col.add(item)

            # check if set is empty
            if set_wrong_values_in_col:
                res_value_not_in_col[f"{table_name}"] = set_wrong_values_in_col

    return None

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
    None
    '''
    # extract set of user cols
    user_cols = [col for col in df_table.columns if col not in OED_COLS]

    # New DataFrame with the selected columns
    user_df = df_table[user_cols]

    # Extract columns that contain one of the search strings and are not in OED_COLS
    miss_ref = {}
    for user_col in user_cols:
        #print(user_col)
        for value in user_df[user_col].astype(str).unique():
            if "[" not in value and "." in value:
                global_table_ref, ref_col = value.split(".")

                # Check if ref_col is not in global_cols and is not a number
                if ref_col not in global_cols and not ref_col.isdigit():
                    miss_ref[user_col] = f"{global_table_ref}.{ref_col}"

    if miss_ref:
        missing_references[table_name] = miss_ref

    return None



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

    tables_paths = return_csv_table_paths(path)

    for table_path in tables_paths:
        table_name = table_path.split("\\")[1].split(".")[0]
        print(table_name)
        df_table = pd.read_csv(
                filepath_or_buffer=table_path,
                sep=",")
        analyse_col_values(table_name, df_table, value_in_col, value_not_in_col)
        check_global_reference_cols(table_name, df_table)


    print(f"Number {sector} tables:", len(tables_paths))
    print("Number of tables with res_value_in_col:", len(res_value_in_col), "res_value_in_col:",res_value_in_col)
    print("res_value_not_in_col:",  res_value_not_in_col)


    print("\nNumber tables with wrong version:", pd.DataFrame.from_dict(res_value_not_in_col, orient='index').shape[0])
    print("\ndf:", pd.DataFrame.from_dict(res_value_not_in_col, orient='index'))

    print("Number tables with wrong version:", pd.DataFrame.from_dict(res_value_not_in_col, orient='index').shape[0],"\nMissing cols", pd.DataFrame.from_dict(table_missing_cols, orient='index'))

    print(missing_references)

    # dicts to dfs
    # Flatten the dictionary
    flattened_data = []
    for outer_key, inner_dict in missing_references.items():
        for inner_key, value in inner_dict.items():
            flattened_data.append({
                'table': outer_key,
                'column': inner_key,
                'reference_table': value.split(".")[0] if "." in value else value,
                'missing_col_in_reference_table': value.split(".")[1] if "." in value else value
            })

    # Convert to DataFrame
    df = pd.DataFrame(flattened_data)
    df.to_csv(f"{sector}_missing_refs.csv")

