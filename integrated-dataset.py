
import pandas as pd

# Load the necessary CSV files
categories_path = "/Users/rui/Downloads/DatabaseSystem/dataset1-split/Categories.csv"
consumption_path = "/Users/rui/Downloads/DatabaseSystem/dataset2-split/FoodConsumption.csv"
preferences_path = "/Users/rui/Downloads/DatabaseSystem/dataset2-split/FoodPreferences.csv"
calorie_path = "/Users/rui/Downloads/DatabaseSystem/dataset2-split/CalorieIntake.csv"
diet_path = "/Users/rui/Downloads/DatabaseSystem/dataset2-split/Diet.csv"

categories = pd.read_csv(categories_path)  # Categories
food_consumption = pd.read_csv(consumption_path)  # FoodConsumption
food_preferences = pd.read_csv(preferences_path)  # FoodPreferences
calorie_intake = pd.read_csv(calorie_path)  # CalorieIntake
diet = pd.read_csv(diet_path)  # Diet

# Repeat the smaller tables to match the size of Categories
num_categories = len(categories)
num_food_consumption = len(food_consumption)
num_food_preferences = len(food_preferences)
num_calorie_intake = len(calorie_intake)
num_diet = len(diet)

# Repeat the smaller tables to match the categories size
food_consumption_repeated = food_consumption.iloc[:num_categories % num_food_consumption].append([food_consumption] * (num_categories // num_food_consumption), ignore_index=True)
food_preferences_repeated = food_preferences.iloc[:num_categories % num_food_preferences].append([food_preferences] * (num_categories // num_food_preferences), ignore_index=True)
calorie_intake_repeated = calorie_intake.iloc[:num_categories % num_calorie_intake].append([calorie_intake] * (num_categories // num_calorie_intake), ignore_index=True)
diet_repeated = diet.iloc[:num_categories % num_diet].append([diet] * (num_categories // num_diet), ignore_index=True)

# Now, create the ProfileCategoryMap by joining Categories and the repeated tables
profile_category_map = []

for i, category_id in enumerate(categories['CategoriesID']):
    profile_category_map.append({
        "ProfileCategoryMapID": i + 1,
        "CategoriesID": category_id,
        "FoodConsumptionID": food_consumption_repeated['FoodConsumptionID'][i],
        "FoodPreferencesID": food_preferences_repeated['FoodPreferencesID'][i],
        "CalorieIntakeID": calorie_intake_repeated['CalorieIntakeID'][i],
        "DietID": diet_repeated['DietID'][i]
    })

# Convert the list to a DataFrame
profile_category_map_df = pd.DataFrame(profile_category_map)

# Save the table to a CSV file
profile_category_map_df.to_csv("/Users/rui/Downloads/DatabaseSystem/ProfileCategoryMap.csv", index=False)

