import pandas as pd
import re

def genrate_usefulCategories(input_path, output_path):
    """ extract main_catefory_en and it's corresponding CategoriesID from Categories.csv
        genrate a new file: usefulCategories.csv """
    userful_data = []
    for chunk in pd.read_csv(input_path, low_memory=False, encoding='utf-8', on_bad_lines="skip", chunksize=10000):
        # Filter rows where 'main_category_en' is not null or does not contain the string "null"
        filtered_chunk = chunk[
            chunk['main_category_en'].notnull() &
            (chunk['main_category_en'] != "null")
        ]
        
        # Append only the required columns to useful_data
        userful_data.append(filtered_chunk[['CategoriesID', 'main_category_en']])

    useful_categories = pd.concat(userful_data, ignore_index=True)
    useful_categories.to_csv(output_path, index=False)# usefulCategories.csv's rows = 1498248

def aggregate_CategoriesID(input_path, output_path):
    # iterate column main_category_en from usefulCategories.csv to calculate the number of unique catogories
    df = pd.read_csv(input_path)
    unique_categories_count = df['main_category_en'].nunique()
    print(f"Number of unique categories in 'main_category_en': {unique_categories_count}")# 58738

    # Group by 'main_category_en' and aggregate all corresponding 'CategoriesID'
    unique_categories = df.groupby('main_category_en')['CategoriesID'].apply(list).reset_index()

    # Rename columns to match the desired table schema
    unique_categories.columns = ['CategoryName', 'CorrespondingCategoriesID']

    unique_categories.to_csv(output_path, index=False)# generate new file UniqueCategory.csv

def filtered_file(input_path, output_path):
    """just keep categories written in english"""
    # input file: UniqueCategory.csv
    df = pd.read_csv(input_path, encoding='utf-8')
    valid_pattern = r"^[A-Za-zÀ-ÿ0-9 ,:-]+$"
    invalid_pattern1 = r"^[0-9, :-]+$"
    invalid_pattern2 = r"^[A-Za-z]{2}:"

    filtered_df = df[
        df['CategoryName'].str.match(valid_pattern, na=False) & 
        ~df['CategoryName'].str.match(invalid_pattern1, na=False) &
        ~df['CategoryName'].str.match(invalid_pattern2, na=False)
    ]
    filtered_df.to_csv(output_path, index=False)
# FilteredUniqueCategory.csv (12.4MB  23365 rows)with just categories written in english, hasn't added primary key
# The reason to generate so many csv files is just for further steps. The only needed one at the end is FilteredUniqueCategory.csv

def matches(input_path1, input_path2, output_path):
    # output file: matches.csv (512kB, 55rows)
    food_preferences_df = pd.read_csv(input_path1, encoding='utf-8')
    filtered_unique_category_df = pd.read_csv(input_path2, encoding='utf-8')

    # extract relevant columns from FoodPreferences
    comfort_food_items = food_preferences_df['comfort_food'].dropna().str.split(',').explode().str.strip().unique()
    food_childhood_items = food_preferences_df['food_childhood'].dropna().str.split(',').explode().str.strip().unique()

    # Combine unique food items from both attributes
    food_preferences_items = set(comfort_food_items).union(set(food_childhood_items))

    # Preprocess the CategoryName column in FilteredUniqueCategory for matching
    filtered_unique_category_df['CategoryName_processed'] = filtered_unique_category_df['CategoryName'].str.lower().str.strip()

    # Find matches between the FoodPreferences items and FilteredUniqueCategory
    matches = filtered_unique_category_df[
        filtered_unique_category_df['CategoryName_processed'].isin(
            [item.lower() for item in food_preferences_items if isinstance(item, str)]
        )
    ]
    matches = matches.drop(columns=['CategoryName_processed'])
    matches.to_csv(output_path, index=False)

path1 = "/Users/rui/Downloads/DatabaseSystem/dataset2-split/FoodPreferences.csv"
path2 = "/Users/rui/Downloads/DatabaseSystem/dataset-integrate/FilteredUniqueCategory.csv"
output_path = "/Users/rui/Downloads/DatabaseSystem/dataset-integrate/matches.csv"
matches(path1, path2, output_path)