import pandas as pd
import psycopg2
from psycopg2 import sql, extras

# Load the dataset
path1 = "/Users/rui/Downloads/DatabaseSystem/FoodConsumptionAndPreferencesDataset.csv"
data = pd.read_csv(path1, sep=',', low_memory=False, encoding='utf-8')

# List all columns in the dataset
columns_food2 = data.columns.tolist()

# Step 1 : classify each column in the raw dataset to its entity 
# Define entity-to-attributes mapping
entity_mapping = {
    "FamilyBackground": [
        {"name": "mother_profession", "type": "VARCHAR"},
        {"name": "mother_education", "type": "DECIMAL"},
        {"name": "father_education", "type": "DECIMAL"},
        {"name": "father_profession", "type": "VARCHAR"},
        {"name": "parents_cook", "type": "DECIMAL"}
    ],
    "FoodConsumption": [
        {"name": "breakfast", "type": "DECIMAL"},
        {"name": "coffee", "type": "DECIMAL"},
        {"name": "veggies_day", "type": "DECIMAL"},
        {"name": "soup", "type": "DECIMAL"},
        {"name": "fruit_day", "type": "DECIMAL"},
        {"name": "fries", "type": "DECIMAL"}
    ],
    "CalorieIntake": [
        {"name": "calories_chicken", "type": "DECIMAL"},
        {"name": "calories_day", "type": "DECIMAL"},
        {"name": "calories_scone", "type": "DECIMAL"},
        {"name": "waffle_calories", "type": "DECIMAL"},
        {"name": "tortilla_calories", "type": "DECIMAL"},
        {"name": "turkey_calories", "type": "DECIMAL"}
    ],
    "FoodPreferences": [
        {"name": "comfort_food", "type": "VARCHAR"},
        {"name": "comfort_food_reasons", "type": "VARCHAR"},
        {"name": "comfort_food_reasons_coded", "type": "DECIMAL"},
        {"name": "cuisine", "type": "DECIMAL"},
        {"name": "drink", "type": "DECIMAL"},
        {"name": "indian_food", "type": "DECIMAL"},
        {"name": "italian_food", "type": "DECIMAL"},
        {"name": "ethnic_food", "type": "DECIMAL"},
        {"name": "fav_food", "type": "DECIMAL"},
        {"name": "fav_cuisine", "type": "VARCHAR"},
        {"name": "fav_cuisine_coded", "type": "DECIMAL"},
        {"name": "greek_food", "type": "DECIMAL"},
        {"name": "persian_food", "type": "DECIMAL"},
        {"name": "thai_food", "type": "DECIMAL"},
        {"name": "food_childhood", "type": "VARCHAR"}
    ],
    "Behaviour": [
        {"name": "cook", "type": "DECIMAL"},
        {"name": "nutritional_check", "type": "DECIMAL"},
        {"name": "vitamins", "type": "DECIMAL"},
        {"name": "healthy_meal", "type": "VARCHAR"},
        {"name": "eating_changes", "type": "VARCHAR"},
        {"name": "eating_changes_coded", "type": "DECIMAL"},
        {"name": "eating_changes_coded1", "type": "DECIMAL"},
        {"name": "eating_out", "type": "DECIMAL"},
        {"name": "exercise", "type": "DECIMAL"},
        {"name": "pay_meal_out", "type": "DECIMAL"},
        {"name": "sports", "type": "DECIMAL"},
        {"name": "type_sports", "type": "VARCHAR"},
        {"name": "meals_dinner_friend", "type": "VARCHAR"}
    ],
    "SelfPerception": [
        {"name": "healthy_feeling", "type": "DECIMAL"},
        {"name": "life_rewarding", "type": "DECIMAL"},
        {"name": "self_perception_weight", "type": "DECIMAL"}
    ],
    "Diet": [
        {"name": "ideal_diet", "type": "VARCHAR"},
        {"name": "ideal_diet_coded", "type": "DECIMAL"},
        {"name": "diet_current", "type": "VARCHAR"},
        {"name": "diet_current_coded", "type": "DECIMAL"}
    ],
    "IndividualProfile": [
        {"name": "GPA", "type": "DECIMAL"},
        {"name": "Gender", "type": "DECIMAL"},
        {"name": "income", "type": "DECIMAL"},
        {"name": "grade_level", "type": "DECIMAL"},
        {"name": "weight", "type": "DECIMAL"},
        {"name": "employment", "type": "DECIMAL"},
        {"name": "on_off_campus", "type": "DECIMAL"},
        {"name": "marital_status", "type": "DECIMAL"}
    ]
}

types = {}
for entity, attributes in entity_mapping.items():
    for attribute in attributes:
        # TODO change varchar to varchar(255)
        types[attribute["name"]] = attribute["type"]


# Classify each column to its entity based on the mapping
classified_attributes = {entity: [] for entity in entity_mapping.keys()}

# Map columns to their respective entities
for column in columns_food2:
    for entity, attributes in entity_mapping.items():
        attribute_names = [attr["name"] for attr in attributes]
        if column in attribute_names:
            classified_attributes[entity].append(column)


#Step 2 : create separate tables for each entity and add primary key
tables = {}
for entity, attributes in entity_mapping.items():
    attribute_names = [attr["name"] for attr in attributes]
    if attribute_names:
        # Create the table with specified columns
        tables[entity] = data[attribute_names].copy()

        # Enforce data types and handle missing values
        for attr in attributes:
            col_name = attr["name"]
            col_type = attr["type"]
            if col_type == "DECIMAL":
                tables[entity][col_name] = pd.to_numeric(
                    tables[entity][col_name].replace("", None), errors="coerce"
                )
            elif col_type == "VARCHAR":
                tables[entity][col_name] = tables[entity][col_name].replace("", "null")
        # Add primary key (auto-incrementing ID)
        tables[entity][f"{entity}ID"] = range(1, len(tables[entity]) + 1)

        


# Step 3: add foreign keys
#Relationships for foreign keys
foreign_key_relationships = {
    "IndividualProfile": [
        "FamilyBackground",
        "FoodConsumption",
        "CalorieIntake",
        "FoodPreferences",
        "Behaviour",
        "SelfPerception",
        "Diet"
    ]
}
# Add foreign keys to IndividualProfile
for related_entity in foreign_key_relationships["IndividualProfile"]:
    tables["IndividualProfile"][f"{related_entity}ID"] = range(
        1, len(tables[related_entity]) + 1
    )



# Step 4: save tables to csv files firstly, later should be integrated with the open food facts tables
for entity, table in tables.items():
    save_path = f"/Users/rui/Downloads/DatabaseSystem/dataset2-split/{entity}.csv"
    table.to_csv(save_path, index=False)
    
#step 5: run the container, command:
#docker run --name database_project -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -e POSTGRES_DB=database_project -p 127.0.0.1:54321:5432 -d postgres
conn = psycopg2.connect(
    dbname="database_project", user="user", password="password", host="127.0.0.1", port=54321
)
conn.autocommit = True
cursor = conn.cursor()       
    
# Define table creation queries based on the schema
create_table_queries = {
    "FamilyBackground": """
        CREATE TABLE FamilyBackground (
            mother_profession VARCHAR(255),
            mother_education DECIMAL,
            father_education DECIMAL,
            father_profession VARCHAR(255),
            parents_cook DECIMAL,
            FamilyBackgroundID INT PRIMARY KEY
        );
    """,
    "FoodConsumption": """
        CREATE TABLE FoodConsumption (
            breakfast DECIMAL,
            coffee DECIMAL,
            veggies_day DECIMAL,
            soup DECIMAL,
            fruit_day DECIMAL,
            fries DECIMAL,
            FoodConsumptionID INT PRIMARY KEY
        );
    """,
    "CalorieIntake": """
        CREATE TABLE CalorieIntake (
            calories_chicken DECIMAL,
            calories_day DECIMAL,
            calories_scone DECIMAL,
            waffle_calories DECIMAL,
            tortilla_calories DECIMAL,
            turkey_calories DECIMAL,
            CalorieIntakeID INT PRIMARY KEY
        );
    """,
    "FoodPreferences": """
        CREATE TABLE FoodPreferences (
            comfort_food VARCHAR(500),
            comfort_food_reasons VARCHAR(500),
            comfort_food_reasons_coded DECIMAL,
            cuisine DECIMAL,
            drink DECIMAL,
            indian_food DECIMAL,
            italian_food DECIMAL,
            ethnic_food DECIMAL,
            fav_food DECIMAL,
            fav_cuisine VARCHAR(255),
            fav_cuisine_coded DECIMAL,
            greek_food DECIMAL,
            persian_food DECIMAL,
            thai_food DECIMAL,
            food_childhood VARCHAR(255),
            FoodPreferencesID INT PRIMARY KEY
        );
    """,
    "Behaviour": """
        CREATE TABLE Behaviour (
            cook DECIMAL,
            nutritional_check DECIMAL,
            vitamins DECIMAL,
            healthy_meal VARCHAR(500),
            eating_changes VARCHAR(500),
            eating_changes_coded DECIMAL,
            eating_changes_coded1 DECIMAL,
            eating_out DECIMAL,
            exercise DECIMAL,
            pay_meal_out DECIMAL,
            sports DECIMAL,
            type_sports VARCHAR(255),
            meals_dinner_friend VARCHAR(500),
            BehaviourID INT PRIMARY KEY
        );
    """,
    "SelfPerception": """
        CREATE TABLE SelfPerception (
            healthy_feeling DECIMAL,
            life_rewarding DECIMAL,
            self_perception_weight DECIMAL,
            SelfPerceptionID INT PRIMARY KEY
        );
    """,
    "Diet": """
        CREATE TABLE Diet (
            ideal_diet VARCHAR(500),
            ideal_diet_coded DECIMAL,
            diet_current VARCHAR(500),
            diet_current_coded DECIMAL,
            DietID INT PRIMARY KEY
        );
    """,
    "IndividualProfile": """
        CREATE TABLE IndividualProfile (
            GPA DECIMAL,
            Gender DECIMAL,
            income DECIMAL,
            grade_level DECIMAL,
            weight DECIMAL,
            employment DECIMAL,
            on_off_campus DECIMAL,
            marital_status DECIMAL,
            IndividualProfileID INT PRIMARY KEY,
            FamilyBackgroundID INT REFERENCES FamilyBackground(FamilyBackgroundID),
            FoodConsumptionID INT REFERENCES FoodConsumption(FoodConsumptionID),
            CalorieIntakeID INT REFERENCES CalorieIntake(CalorieIntakeID),
            FoodPreferencesID INT REFERENCES FoodPreferences(FoodPreferencesID),
            BehaviourID INT REFERENCES Behaviour(BehaviourID),
            SelfPerceptionID INT REFERENCES SelfPerception(SelfPerceptionID),
            DietID INT REFERENCES Diet(DietID)
        );
    """
}

# Drop and recreate tables
def recreate_tables():
    for entity in reversed(create_table_queries.keys()):  # Drop tables in reverse order of creation to handle dependencies
        drop_query = f"DROP TABLE IF EXISTS {entity} CASCADE;"
        cursor.execute(drop_query)
        print(f"Dropped table {entity} if it existed.")

    for entity, create_query in create_table_queries.items():
        cursor.execute(create_query)
        print(f"Created table {entity}.")

# Load CSV files into tables
def load_csv_to_table(file_path, table_name):
    with open(file_path, 'r') as file:
        next(file)  # Skip the header
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", file)
        print(f"Data loaded into {table_name}.")


# Main execution
try:
    recreate_tables()  # Ensure all tables are created

    # Insert data into each table
    for entity in create_table_queries.keys():
        file_path = f"/Users/rui/Downloads/DatabaseSystem/dataset2-split/{entity}.csv"
        load_csv_to_table(file_path, entity)

    conn.commit()
    print("All data inserted successfully!")
except Exception as e:
    conn.rollback()
    print("Error inserting data:", e)
finally:
    cursor.close()
    conn.close()

