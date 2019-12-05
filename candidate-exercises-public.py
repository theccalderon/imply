import pandas as pd
import pandavro as pdx
from pathlib import Path
import sys


#Cell
def combine_files(json_filepath, arvo_filepath, csv_filepath, output_filepath):
    """Combines the three files, eliminates duplicates and it sorts the resulting dataset by City Name. Creates
    a csv file in output_filepath and returns a dataframe with its content
    """
    #reading all the three files
    df = pd.read_json(json_filepath)
    df = df.append(pd.read_csv(csv_filepath))
    df = df.append(pdx.read_avro(arvo_filepath))
    #dropping duplicates
    df = df.drop_duplicates()
    #sorting by Name
    df = df.sort_values(by='Name')
    #writing to csv
    df.to_csv(output_filepath,columns=['Name','CountryCode','Population'], index=False)
    return pd.read_csv(output_filepath)


#Cell
def city_with_largets_population_from_df(df):
    """Returns the city with the largest population
    """
    return df.sort_values(by='Population',ascending=False)[:1][['Name']].iloc[0]['Name']


#Cell
def total_population_per_country_from_df(df, country_code):
    """Returns the total population for country_code
    """
    return df.loc[df['CountryCode'] == country_code].groupby('CountryCode').Population.sum().iloc[0]


def main():
    if len(sys.argv) != 5:
        print("Wrong usage, try: python candidate-exercises-public.py path/to/json/file path/to/avro/file "
              "path/to/csv/file "
              "path/to/final/csv/file")
        sys.exit(1)
    print("Combining the files, eliminating any duplicates and write to a single .CSV file sorted alphabetically by "
          "the city name.")
    final_csv = combine_files(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    print("The file "+sys.argv[4] + " was created successfully")
    print("What is the count of all rows?")
    print(len(final_csv))
    print("What is the city with the largest population?")
    print(city_with_largets_population_from_df(final_csv))
    print("What is the total population of all cities in Brazil (CountryCode == BRA)?")
    print(total_population_per_country_from_df(final_csv, 'BRA'))
    sys.exit(0)


if __name__ == "__main__":
    main()
