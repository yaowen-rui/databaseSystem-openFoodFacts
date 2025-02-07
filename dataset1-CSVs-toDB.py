import psycopg2
import csv
import pandas as pd
from psycopg2.extras import execute_batch

entity_mapping = {
    "ContributionHistory": [
        {"name": "creator", "type": "VARCHAR"},
        {"name": "created_t", "type": "DECIMAL"},
        {"name": "created_datetime", "type": "VARCHAR"},
        {"name": "last_modified_t", "type": "DECIMAL"},
        {"name": "last_modified_datetime", "type": "VARCHAR"},
        {"name": "last_modified_by", "type": "VARCHAR"},
        {"name": "last_updated_t", "type": "DECIMAL"},
        {"name": "last_updated_datetime", "type": "VARCHAR"}
    ],
    "PackagingInfo": [
        {"name": "quantity", "type": "VARCHAR"},
        {"name": "packaging", "type": "VARCHAR"},
        {"name": "packaging_tags", "type": "VARCHAR"},
        {"name": "packaging_en", "type": "VARCHAR"},
        {"name": "packaging_text", "type": "VARCHAR"},
        {"name": "states", "type": "VARCHAR"},
        {"name": "states_tags", "type": "VARCHAR"},
        {"name": "states_en", "type": "VARCHAR"},
        {"name": "product_quantity", "type": "DECIMAL"},
    ],
    "Labeling": [
        {"name": "labels", "type": "VARCHAR"},
        {"name": "labels_tags", "type": "VARCHAR"},
        {"name": "labels_en", "type": "VARCHAR"}
    ],
    "Categories": [
        {"name": "categories", "type": "VARCHAR"},
        {"name": "categories_tags", "type": "VARCHAR"},
        {"name": "categories_en", "type": "VARCHAR"},
        {"name": "main_category", "type": "VARCHAR"},
        {"name": "main_category_en", "type": "VARCHAR"}
    ],
    "Origins": [
        {"name": "origins", "type": "VARCHAR"},
        {"name": "origins_tags", "type": "VARCHAR"},
        {"name": "origins_en", "type": "VARCHAR"}
    ],
    "ManufacturingDetail": [
        {"name": "manufacturing_places", "type": "VARCHAR"},
        {"name": "manufacturing_places_tags", "type": "VARCHAR"},
        {"name": "emb_codes", "type": "VARCHAR"},
        {"name": "emb_codes_tags", "type": "VARCHAR"},
        {"name": "first_packaging_code_geo", "type": "VARCHAR"}
    ],
    "PurchasesInfo": [
        {"name": "cities", "type": "VARCHAR"},
        {"name": "cities_tags", "type": "VARCHAR"},
        {"name": "purchase_places", "type": "VARCHAR"},
        {"name": "stores", "type": "VARCHAR"},
        {"name": "countries", "type": "VARCHAR"},
        {"name": "countries_tags", "type": "VARCHAR"},
        {"name": "countries_en", "type": "VARCHAR"}
    ],
    "Ingredients": [
        {"name": "ingredients_text", "type": "VARCHAR"},
        {"name": "ingredients_tags", "type": "VARCHAR"},
        {"name": "ingredients_analysis_tags", "type": "VARCHAR"}
    ],
    "AllergensAndTraces": [
        {"name": "allergens", "type": "VARCHAR"},
        {"name": "allergens_en", "type": "VARCHAR"},
        {"name": "traces", "type": "VARCHAR"},
        {"name": "traces_tags", "type": "VARCHAR"},
        {"name": "traces_en", "type": "VARCHAR"},
    ],
    "Additives": [
        {"name": "additives_n", "type": "INT"},
        {"name": "additives", "type": "VARCHAR"},
        {"name": "additives_tags", "type": "VARCHAR"},
        {"name": "additives_en", "type": "VARCHAR"}
    ],
    "EnvironmentalInfo": [
        {"name": "ecoscore_score", "type": "INT"},
        {"name": "ecoscore_grade", "type": "VARCHAR"},
        {"name": "nutrient_levels_tags", "type": "VARCHAR"},
        {"name": "carbon-footprint_100g", "type": "DECIMAL"},
        {"name": "carbon-footprint-from-meat-or-fish_100g", "type": "DECIMAL"}
    ],
    "ImageInfo": [
        {"name": "image_url", "type": "VARCHAR"},
        {"name": "image_small_url", "type": "VARCHAR"},
        {"name": "image_ingredients_url", "type": "VARCHAR"},
        {"name": "image_ingredients_small_url", "type": "VARCHAR"},
        {"name": "image_nutrition_url", "type": "VARCHAR"},
        {"name": "image_nutrition_small_url", "type": "VARCHAR"},
        {"name": "last_image_t", "type": "VARCHAR"},
        {"name": "last_image_datetime", "type": "VARCHAR"}
    ],
    "MiscellaneousInfo": [
        {"name": "owner", "type": "VARCHAR"},
        {"name": "data_quality_errors_tags", "type": "VARCHAR"},
        {"name": "unique_scans_n", "type": "INT"},
        {"name": "popularity_tags", "type": "VARCHAR"},
        {"name": "completeness", "type": "DECIMAL"}
    ],
    "EnergyAndFat": [
        {"name": "energy-kj_100g", "type": "DECIMAL"},
        {"name": "energy-kcal_100g", "type": "DECIMAL"},
        {"name": "energy_100g", "type": "DECIMAL"},
        {"name": "energy-from-fat_100g", "type": "DECIMAL"}
    ],
    "FattyInfo": [
        {"name": "fat_100g", "type": "DECIMAL"},
        {"name": "saturated-fat_100g", "type": "DECIMAL"},
        {"name": "butyric-acid_100g", "type": "DECIMAL"},
        {"name": "caproic-acid_100g", "type": "DECIMAL"},
        {"name": "caprylic-acid_100g", "type": "DECIMAL"},
        {"name": "capric-acid_100g", "type": "DECIMAL"},
        {"name": "lauric-acid_100g", "type": "DECIMAL"},
        {"name": "myristic-acid_100g", "type": "DECIMAL"},
        {"name": "palmitic-acid_100g", "type": "DECIMAL"},
        {"name": "stearic-acid_100g", "type": "DECIMAL"},
        {"name": "arachidic-acid_100g", "type": "DECIMAL"},
        {"name": "behenic-acid_100g", "type": "DECIMAL"},
        {"name": "lignoceric-acid_100g", "type": "DECIMAL"},
        {"name": "cerotic-acid_100g", "type": "DECIMAL"},
        {"name": "montanic-acid_100g", "type": "DECIMAL"},
        {"name": "melissic-acid_100g", "type": "DECIMAL"},
        {"name": "unsaturated-fat_100g", "type": "DECIMAL"},
        {"name": "monounsaturated-fat_100g", "type": "DECIMAL"},
        {"name": "omega-9-fat_100g", "type": "DECIMAL"},
        {"name": "polyunsaturated-fat_100g", "type": "DECIMAL"},
        {"name": "omega-3-fat_100g", "type": "DECIMAL"},
        {"name": "omega-6-fat_100g", "type": "DECIMAL"},
        {"name": "alpha-linolenic-acid_100g", "type": "DECIMAL"},
        {"name": "eicosapentaenoic-acid_100g", "type": "DECIMAL"},
        {"name": "docosahexaenoic-acid_100g", "type": "DECIMAL"},
        {"name": "linoleic-acid_100g", "type": "DECIMAL"},
        {"name": "arachidonic-acid_100g", "type": "DECIMAL"},
        {"name": "gamma-linolenic-acid_100g", "type": "DECIMAL"},
        {"name": "dihomo-gamma-linolenic-acid_100g", "type": "DECIMAL"},
        {"name": "oleic-acid_100g", "type": "DECIMAL"},
        {"name": "elaidic-acid_100g", "type": "DECIMAL"},
        {"name": "gondoic-acid_100g", "type": "DECIMAL"},
        {"name": "mead-acid_100g", "type": "DECIMAL"},
        {"name": "erucic-acid_100g", "type": "DECIMAL"},
        {"name": "nervonic-acid_100g", "type": "DECIMAL"},
        {"name": "trans-fat_100g", "type": "DECIMAL"},
        {"name": "cholesterol_100g", "type": "DECIMAL"}
    ],
    "Carbohydrates": [
        {"name": "carbohydrates_100g", "type": "DECIMAL"},
        {"name": "sugars_100g", "type": "DECIMAL"},
        {"name": "added-sugars_100g", "type": "DECIMAL"},
        {"name": "sucrose_100g", "type": "DECIMAL"},
        {"name": "glucose_100g", "type": "DECIMAL"},
        {"name": "fructose_100g", "type": "DECIMAL"},
        {"name": "lactose_100g", "type": "DECIMAL"},
        {"name": "maltose_100g", "type": "DECIMAL"},
        {"name": "maltodextrins_100g", "type": "DECIMAL"},
        {"name": "starch_100g", "type": "DECIMAL"},
        {"name": "polyols_100g", "type": "DECIMAL"},
        {"name": "erythritol_100g", "type": "DECIMAL"}
    ],
    "Proteins": [
        {"name": "proteins_100g", "type": "DECIMAL"},
        {"name": "casein_100g", "type": "DECIMAL"},
        {"name": "serum-proteins_100g", "type": "DECIMAL"},
        {"name": "nucleotides_100g", "type": "DECIMAL"}
    ],
    "Fiber": [
        {"name": "fiber_100g", "type": "DECIMAL"},
        {"name": "soluble-fiber_100g", "type": "DECIMAL"},
        {"name": "insoluble-fiber_100g", "type": "DECIMAL"},
        {"name": "beta-glucan_100g", "type": "DECIMAL"}
    ],
    "Minerals": [
        {"name": "salt_100g", "type": "DECIMAL"},
        {"name": "added-salt_100g", "type": "DECIMAL"},
        {"name": "sodium_100g", "type": "DECIMAL"},
        {"name": "iron_100g", "type": "DECIMAL"},
        {"name": "magnesium_100g", "type": "DECIMAL"},
        {"name": "zinc_100g", "type": "DECIMAL"},
        {"name": "copper_100g", "type": "DECIMAL"},
        {"name": "calcium_100g", "type": "DECIMAL"},
        {"name": "iodine_100g", "type": "DECIMAL"},
        {"name": "silica_100g", "type": "DECIMAL"},
        {"name": "phosphorus_100g", "type": "DECIMAL"},
        {"name": "manganese_100g", "type": "DECIMAL"},
        {"name": "selenium_100g", "type": "DECIMAL"},
        {"name": "potassium_100g", "type": "DECIMAL"},
        {"name": "chloride_100g", "type": "DECIMAL"},
        {"name": "fluoride_100g", "type": "DECIMAL"},
        {"name": "chromium_100g", "type": "DECIMAL"},
        {"name": "molybdenum_100g", "type": "DECIMAL"},
        {"name": "bicarbonate_100g", "type": "DECIMAL"}
    ],
    "Vitamins": [
        {"name": "vitamin-a_100g", "type": "DECIMAL"},
        {"name": "beta-carotene_100g", "type": "DECIMAL"},
        {"name": "vitamin-d_100g", "type": "DECIMAL"},
        {"name": "vitamin-e_100g", "type": "DECIMAL"},
        {"name": "vitamin-k_100g", "type": "DECIMAL"},
        {"name": "vitamin-c_100g", "type": "DECIMAL"},
        {"name": "vitamin-b1_100g", "type": "DECIMAL"},
        {"name": "vitamin-b2_100g", "type": "DECIMAL"},
        {"name": "vitamin-pp_100g", "type": "DECIMAL"},
        {"name": "vitamin-b6_100g", "type": "DECIMAL"},
        {"name": "vitamin-b9_100g", "type": "DECIMAL"},
        {"name": "vitamin-b12_100g", "type": "DECIMAL"}
    ],
    "VitaminDerivatives": [
        {"name": "biotin_100g", "type": "DECIMAL"},
        {"name": "pantothenic-acid_100g", "type": "DECIMAL"},
        {"name": "folates_100g", "type": "DECIMAL"},
        {"name": "phylloquinone_100g", "type": "DECIMAL"},
        {"name": "choline_100g", "type": "DECIMAL"}
    ],
    "NutritionalCompositionAndScores": [
        {"name": "fruits-vegetables-nuts_100g", "type": "DECIMAL"},
        {"name": "fruits-vegetables-nuts-dried_100g", "type": "DECIMAL"},
        {"name": "fruits-vegetables-nuts-estimate_100g", "type": "DECIMAL"},
        {"name": "fruits-vegetables-nuts-estimate-from-ingredients_100g", "type": "DECIMAL"},
        {"name": "collagen-meat-protein-ratio_100g", "type": "DECIMAL"},
        {"name": "cocoa_100g", "type": "DECIMAL"},
        {"name": "chlorophyl_100g", "type": "DECIMAL"},
        {"name": "nutrition-score-fr_100g", "type": "DECIMAL"},
        {"name": "nutrition-score-uk_100g", "type": "DECIMAL"},
    ],
    "BioactiveCompounds": [
        {"name": "caffeine_100g", "type": "DECIMAL"},
        {"name": "taurine_100g", "type": "DECIMAL"},
        {"name": "nitrate_100g", "type": "DECIMAL"},
        {"name": "sulphate_100g", "type": "DECIMAL"},
        {"name": "carnitine_100g", "type": "DECIMAL"},
        {"name": "inositol_100g", "type": "DECIMAL"},
        {"name": "alcohol_100g", "type": "DECIMAL"}
    ],
    "PhysicalAndChemicalProperties": [
        {"name": "ph_100g", "type": "DECIMAL"},
        {"name": "glycemic-index_100g", "type": "DECIMAL"},
        {"name": "water-hardness_100g", "type": "DECIMAL"},
        {"name": "acidity_100g", "type": "DECIMAL"}
    ],
    "NutritionMeta": [
        {"name": "serving_size", "type": "VARCHAR"},
        {"name": "serving_quantity", "type": "DECIMAL"},
        {"name": "no_nutrition_data", "type": "VARCHAR"},
        {"name": "nutriscore_score", "type": "DECIMAL"},
        {"name": "nutriscore_grade", "type": "VARCHAR"},
        {"name": "nova_group", "type": "DECIMAL"},
        {"name": "pnns_groups_1", "type": "VARCHAR"},
        {"name": "pnns_groups_2", "type": "VARCHAR"},
        {"name": "food_groups", "type": "VARCHAR"},
        {"name": "food_groups_tags", "type": "VARCHAR"},
        {"name": "food_groups_en", "type": "VARCHAR"}
    ],
    "Products": [
        {"name": "code", "type": "VARCHAR"},
        {"name": "url", "type": "VARCHAR"},
        {"name": "product_name", "type": "VARCHAR"},
        {"name": "abbreviated_product_name", "type": "VARCHAR"},
        {"name": "generic_name", "type": "VARCHAR"},
        {"name": "brands", "type": "VARCHAR"},
        {"name": "brands_tags", "type": "VARCHAR"},
        {"name": "brand_owner", "type": "VARCHAR"}
    ]
}
# somehow foreign keys are not added successfully, use the script "add-foreign-keys-inDB.py"
foreign_keys = {
    "Products": [
        "LabelingID", "PackagingInfoID", "OriginsID", "ManufacturingDetailID", 
        "ContributionHistoryID", "PurchasesInfoID", "AdditivesID", 
        "EnvironmentalInfoID", "ImageInfoID", "MiscellaneousInfoID", 
        "AllergensAndTracesID", "NutritionMetaID", "CategoriesID", 
        "IngredientsID"
    ],
    "NutritionMeta": [
        "BioactiveCompoundsID", "EnergyAndFatID", "FiberID", 
        "PhysicalAndChemicalPropertiesID", "FattyInfoID", "MineralsID", 
        "CarbohydratesID", "VitaminDerivativesID", "ProteinsID", 
        "VitaminsID", "NutritionalCompositionAndScoresID"
    ]
}

def handle_special_values(df):
    """handle empty value and some unexpected formats that do not match the schema """
    #return df.where(pd.notnull(df), None)
    for col in df.columns:
        if col == 'additives_n':  # Special handling for additives_n in additivies.csv
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert invalid strings to NaN
            df[col] = df[col].fillna(0).astype(int)  # Replace NaN with 0 and cast to int
        elif df[col].dtype.kind in {'i', 'u', 'f'}:  # Integer, unsigned, or float
            df[col] = df[col].fillna(0)  # Replace NaN with 0
        elif col in ['serving_quantity', 'nutriscore_score', 'nova_group']:  # special handling these 3 from nutritionalmeta.csv
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert invalid values to NaN
        else:
            df[col] = df[col].where(pd.notnull(df[col]), None)  # Replace NaN with None for other fields
    return df


def sanitize_entity_mapping(entity_mapping):
    sanitized_mapping = {}
    for entity, columns in entity_mapping.items():
        sanitized_columns = []
        for column in columns:
            sanitized_column = {
                "name": column["name"].replace("-", "_").lower(),# lowcase entity name
                "type": column["type"]
            }
            sanitized_columns.append(sanitized_column)
        sanitized_mapping[entity.lower()] = sanitized_columns
    return sanitized_mapping

entity_mapping = sanitize_entity_mapping(entity_mapping)


def generate_create_table_queries(entity_mapping, foreign_keys):
    create_table_queries = {}
    for entity, columns in entity_mapping.items():
        column_definitions = []

        # Define columns
        for column in columns:
            column_name = column['name']
            column_type = column['type']
            
            if column_type == "VARCHAR":
                column_type = "VARCHAR"
            elif column_type == "DECIMAL":
                column_type = "DECIMAL"
            elif column_type == "INT":
                column_type = "INT"
            column_definitions.append(f"{column_name} {column_type}")
        
        # Add primary key
        column_definitions.append(f"{entity}id SERIAL PRIMARY KEY")
        
        # Add foreign keys if applicable
        if entity in foreign_keys:
            for fk in foreign_keys[entity]:
                column_definitions.append(f"{fk.lower()} INT REFERENCES {fk[:-2].lower()}({fk.lower()})")
        
        # Combine definitions into a CREATE TABLE statement
        column_definitions_str = ", ".join(column_definitions)
        create_query = f"CREATE TABLE {entity} ({column_definitions_str});"
        create_table_queries[entity] = {
            "query": create_query
        }
    
    return create_table_queries

# Database connection
conn = psycopg2.connect(
    dbname="database_project", user="user", password="password", host="127.0.0.1", port=54321
)
conn.autocommit = True
cursor = conn.cursor()

# Define table creation queries based on the schema
create_table_queries = generate_create_table_queries(entity_mapping, foreign_keys)


# Drop and recreate tables
def recreate_tables():
    for entity in reversed(create_table_queries.keys()):  # Drop tables in reverse order of creation to handle dependencies
        drop_query = f"DROP TABLE IF EXISTS {entity} CASCADE;"
        cursor.execute(drop_query)
        print(f"Dropped table {entity} if it existed.")

    for entity, query_info in create_table_queries.items():
        cursor.execute(query_info["query"])
        print(f"Created table {entity}.")

def sanitize_headers(headers):
    return [header.replace("-", "_").lower() for header in headers]

def assign_sequential_ids(df, fk_columns):
    for fk_column in fk_columns:
        df[fk_column] = range(1, len(df) + 1)
    return df

# Load CSV files into tables using chunks
def load_csv_to_table_in_chunks(file_path, table_name):
    table_name = table_name.lower()
    
    # Read CSV in chunks
    for chunk in pd.read_csv(file_path, low_memory=False, chunksize=20000, encoding="utf-8", on_bad_lines="skip"):
        chunk.columns = sanitize_headers(chunk.columns)  # Sanitize headers
        chunk = handle_special_values(chunk)  # Handle empty values

        # Convert DataFrame to list of tuples for insertion
        rows = [tuple(row) for row in chunk.to_numpy()]
        # Debug: Check the structure of rows and columns
        #print(f"Inserting into table {table_name}:")
        #print(f"Columns: {', '.join(column_lengths.keys())}")
        #print(f"Sample row: {rows[0]}")  # Show a sample row for debugging
        
        try:
            # Insert data using execute_batch for efficiency
            placeholders = ", ".join(["%s"] * len(chunk.columns))
            insert_query = f"INSERT INTO {table_name} ({', '.join(chunk.columns)}) VALUES ({placeholders})"
            execute_batch(cursor, insert_query, rows)
        except Exception as e:

            print(f"Error during insertion: {e}")
            print(f"Offending query: {insert_query}")
            print(f"Sample rows: {rows[:2]}")
            raise  # Re-raise the exception after logging


# Main execution
try:
    recreate_tables()  # Ensure all tables are created

    # Insert data into each table
    for entity in create_table_queries.keys():
        file_path = f"/Users/rui/Downloads/DatabaseSystem/dataset1-split/{entity}.csv"
        load_csv_to_table_in_chunks(file_path, entity)
        print(f"{entity}.csv is inserted into DB")

    conn.commit()
    print("All data inserted successfully!")
except Exception as e:
    conn.rollback()
    print("Error inserting data:", e)
finally:
    cursor.close()
    conn.close()
