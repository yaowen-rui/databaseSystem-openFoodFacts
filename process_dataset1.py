import pandas as pd
import psycopg2
from sqlalchemy import create_engine, VARCHAR, DECIMAL, VARCHAR
import re


def process_dataset1():
    # Step 1 : classify each column in the raw dataset to its entity 
    # Define entity-to-attributes mapping
    entity_mapping = {
        "ContributionHistory": [
            {"name": "creator", "type": VARCHAR},
            {"name": "created_t", "type": VARCHAR},
            {"name": "created_datetime", "type": VARCHAR},
            {"name": "last_modified_t", "type": VARCHAR},
            {"name": "last_modified_datetime", "type": VARCHAR},
            {"name": "last_modified_by", "type": VARCHAR},
            {"name": "last_updated_t", "type": VARCHAR},
            {"name": "last_updated_datetime", "type": VARCHAR}
        ],
        "PackagingInfo": [
            {"name": "quantity", "type": VARCHAR},
            {"name": "product_quantity", "type": DECIMAL},
            {"name": "packaging", "type": VARCHAR},
            {"name": "packaging_tags", "type": VARCHAR},
            {"name": "packaging_en", "type": VARCHAR},
            {"name": "packaging_text", "type": VARCHAR},
            {"name": "states", "type": VARCHAR},
            {"name": "states_tags", "type": VARCHAR},
            {"name": "states_en", "type": VARCHAR}
        ],
        "Labeling": [
            {"name": "labels", "type": VARCHAR},
            {"name": "labels_tags", "type": VARCHAR},
            {"name": "labels_en", "type": VARCHAR}
        ],
        "Categories": [
            {"name": "categories", "type": VARCHAR},
            {"name": "categories_tags", "type": VARCHAR},
            {"name": "categories_en", "type": VARCHAR},
            {"name": "main_category", "type": VARCHAR},
            {"name": "main_category_en", "type": VARCHAR}
        ],
        "Origins": [
            {"name": "origins", "type": VARCHAR},
            {"name": "origins_tags", "type": VARCHAR},
            {"name": "origins_en", "type": VARCHAR}
        ],
        "ManufacturingDetail": [
            {"name": "manufacturing_places", "type": VARCHAR},
            {"name": "manufacturing_places_tags", "type": VARCHAR},
            {"name": "emb_codes", "type": VARCHAR},
            {"name": "emb_codes_tags", "type": VARCHAR},
            {"name": "first_packaging_code_geo", "type": VARCHAR}
        ],
        "PurchasesInfo": [
            {"name": "cities", "type": VARCHAR},
            {"name": "cities_tags", "type": VARCHAR},
            {"name": "purchase_places", "type": VARCHAR},
            {"name": "stores", "type": VARCHAR},
            {"name": "countries", "type": VARCHAR},
            {"name": "countries_tags", "type": VARCHAR},
            {"name": "countries_en", "type": VARCHAR}
        ],
        "Ingredients": [
            {"name": "ingredients_text", "type": VARCHAR},
            {"name": "ingredients_tags", "type": VARCHAR},
            {"name": "ingredients_analysis_tags", "type": VARCHAR}
        ],
        "AllergensAndTraces": [
            {"name": "allergens", "type": VARCHAR},
            {"name": "allergens_en", "type": VARCHAR},
            {"name": "traces", "type": VARCHAR},
            {"name": "traces_tags", "type": VARCHAR},
            {"name": "traces_en", "type": VARCHAR},
        ],
        "Additives": [
            {"name": "additives_n", "type": DECIMAL},
            {"name": "additives", "type": VARCHAR},
            {"name": "additives_tags", "type": VARCHAR},
            {"name": "additives_en", "type": VARCHAR}
        ],
        "EnvironmentalInfo": [
            {"name": "ecoscore_score", "type": DECIMAL},
            {"name": "ecoscore_grade", "type": VARCHAR},
            {"name": "nutrient_levels_tags", "type": VARCHAR},
            {"name": "carbon-footprint_100g", "type": DECIMAL},
            {"name": "carbon-footprint-from-meat-or-fish_100g", "type": DECIMAL}
        ],
        "ImageInfo": [
            {"name": "image_url", "type": VARCHAR},
            {"name": "image_small_url", "type": VARCHAR},
            {"name": "image_ingredients_url", "type": VARCHAR},
            {"name": "image_ingredients_small_url", "type": VARCHAR},
            {"name": "image_nutrition_url", "type": VARCHAR},
            {"name": "image_nutrition_small_url", "type": VARCHAR},
            {"name": "last_image_t", "type": VARCHAR},
            {"name": "last_image_datetime", "type": VARCHAR}
        ],
        "MiscellaneousInfo": [
            {"name": "owner", "type": VARCHAR},
            {"name": "data_quality_errors_tags", "type": VARCHAR},
            {"name": "unique_scans_n", "type": DECIMAL},
            {"name": "popularity_tags", "type": VARCHAR},
            {"name": "completeness", "type": DECIMAL}
        ],
        "EnergyAndFat": [
            {"name": "energy-kj_100g", "type": DECIMAL},
            {"name": "energy-kcal_100g", "type": DECIMAL},
            {"name": "energy_100g", "type": DECIMAL},
            {"name": "energy-from-fat_100g", "type": DECIMAL}
        ],
        "FattyInfo": [
            {"name": "fat_100g", "type": DECIMAL},
            {"name": "saturated-fat_100g", "type": DECIMAL},
            {"name": "butyric-acid_100g", "type": DECIMAL},
            {"name": "caproic-acid_100g", "type": DECIMAL},
            {"name": "caprylic-acid_100g", "type": DECIMAL},
            {"name": "capric-acid_100g", "type": DECIMAL},
            {"name": "lauric-acid_100g", "type": DECIMAL},
            {"name": "myristic-acid_100g", "type": DECIMAL},
            {"name": "palmitic-acid_100g", "type": DECIMAL},
            {"name": "stearic-acid_100g", "type": DECIMAL},
            {"name": "arachidic-acid_100g", "type": DECIMAL},
            {"name": "behenic-acid_100g", "type": DECIMAL},
            {"name": "lignoceric-acid_100g", "type": DECIMAL},
            {"name": "cerotic-acid_100g", "type": DECIMAL},
            {"name": "montanic-acid_100g", "type": DECIMAL},
            {"name": "melissic-acid_100g", "type": DECIMAL},
            {"name": "unsaturated-fat_100g", "type": DECIMAL},
            {"name": "monounsaturated-fat_100g", "type": DECIMAL},
            {"name": "omega-9-fat_100g", "type": DECIMAL},
            {"name": "polyunsaturated-fat_100g", "type": DECIMAL},
            {"name": "omega-3-fat_100g", "type": DECIMAL},
            {"name": "omega-6-fat_100g", "type": DECIMAL},
            {"name": "alpha-linolenic-acid_100g", "type": DECIMAL},
            {"name": "eicosapentaenoic-acid_100g", "type": DECIMAL},
            {"name": "docosahexaenoic-acid_100g", "type": DECIMAL},
            {"name": "linoleic-acid_100g", "type": DECIMAL},
            {"name": "arachidonic-acid_100g", "type": DECIMAL},
            {"name": "gamma-linolenic-acid_100g", "type": DECIMAL},
            {"name": "dihomo-gamma-linolenic-acid_100g", "type": DECIMAL},
            {"name": "oleic-acid_100g", "type": DECIMAL},
            {"name": "elaidic-acid_100g", "type": DECIMAL},
            {"name": "gondoic-acid_100g", "type": DECIMAL},
            {"name": "mead-acid_100g", "type": DECIMAL},
            {"name": "erucic-acid_100g", "type": DECIMAL},
            {"name": "nervonic-acid_100g", "type": DECIMAL},
            {"name": "trans-fat_100g", "type": DECIMAL},
            {"name": "cholesterol_100g", "type": DECIMAL}
        ],
        "Carbohydrates": [
            {"name": "carbohydrates_100g", "type": DECIMAL},
            {"name": "sugars_100g", "type": DECIMAL},
            {"name": "added-sugars_100g", "type": DECIMAL},
            {"name": "sucrose_100g", "type": DECIMAL},
            {"name": "glucose_100g", "type": DECIMAL},
            {"name": "fructose_100g", "type": DECIMAL},
            {"name": "lactose_100g", "type": DECIMAL},
            {"name": "maltose_100g", "type": DECIMAL},
            {"name": "maltodextrins_100g", "type": DECIMAL},
            {"name": "starch_100g", "type": DECIMAL},
            {"name": "polyols_100g", "type": DECIMAL},
            {"name": "erythritol_100g", "type": DECIMAL}
        ],
        "Proteins": [
            {"name": "proteins_100g", "type": DECIMAL},
            {"name": "casein_100g", "type": DECIMAL},
            {"name": "serum-proteins_100g", "type": DECIMAL},
            {"name": "nucleotides_100g", "type": DECIMAL}
        ],
        "Fiber": [
            {"name": "fiber_100g", "type": DECIMAL},
            {"name": "soluble-fiber_100g", "type": DECIMAL},
            {"name": "insoluble-fiber_100g", "type": DECIMAL},
            {"name": "beta-glucan_100g", "type": DECIMAL}
        ],
        "Minerals": [
            {"name": "salt_100g", "type": DECIMAL},
            {"name": "added-salt_100g", "type": DECIMAL},
            {"name": "sodium_100g", "type": DECIMAL},
            {"name": "iron_100g", "type": DECIMAL},
            {"name": "magnesium_100g", "type": DECIMAL},
            {"name": "zinc_100g", "type": DECIMAL},
            {"name": "copper_100g", "type": DECIMAL},
            {"name": "calcium_100g", "type": DECIMAL},
            {"name": "iodine_100g", "type": DECIMAL},
            {"name": "silica_100g", "type": DECIMAL},
            {"name": "phosphorus_100g", "type": DECIMAL},
            {"name": "manganese_100g", "type": DECIMAL},
            {"name": "selenium_100g", "type": DECIMAL},
            {"name": "potassium_100g", "type": DECIMAL},
            {"name": "chloride_100g", "type": DECIMAL},
            {"name": "fluoride_100g", "type": DECIMAL},
            {"name": "chromium_100g", "type": DECIMAL},
            {"name": "molybdenum_100g", "type": DECIMAL},
            {"name": "bicarbonate_100g", "type": DECIMAL}
        ],
        "Vitamins": [
            {"name": "vitamin-a_100g", "type": DECIMAL},
            {"name": "beta-carotene_100g", "type": DECIMAL},
            {"name": "vitamin-d_100g", "type": DECIMAL},
            {"name": "vitamin-e_100g", "type": DECIMAL},
            {"name": "vitamin-k_100g", "type": DECIMAL},
            {"name": "vitamin-c_100g", "type": DECIMAL},
            {"name": "vitamin-b1_100g", "type": DECIMAL},
            {"name": "vitamin-b2_100g", "type": DECIMAL},
            {"name": "vitamin-pp_100g", "type": DECIMAL},
            {"name": "vitamin-b6_100g", "type": DECIMAL},
            {"name": "vitamin-b9_100g", "type": DECIMAL},
            {"name": "vitamin-b12_100g", "type": DECIMAL}
        ],
        "VitaminDerivatives": [
            {"name": "biotin_100g", "type": DECIMAL},
            {"name": "pantothenic-acid_100g", "type": DECIMAL},
            {"name": "folates_100g", "type": DECIMAL},
            {"name": "phylloquinone_100g", "type": DECIMAL},
            {"name": "choline_100g", "type": DECIMAL}
        ],
        "NutritionalCompositionAndScores": [
            {"name": "fruits-vegetables-nuts_100g", "type": DECIMAL},
            {"name": "fruits-vegetables-nuts-dried_100g", "type": DECIMAL},
            {"name": "fruits-vegetables-nuts-estimate_100g", "type": DECIMAL},
            {"name": "fruits-vegetables-nuts-estimate-from-ingredients_100g", "type": DECIMAL},
            {"name": "collagen-meat-protein-ratio_100g", "type": DECIMAL},
            {"name": "cocoa_100g", "type": DECIMAL},
            {"name": "chlorophyl_100g", "type": DECIMAL},
            {"name": "nutrition-score-fr_100g", "type": DECIMAL},
            {"name": "nutrition-score-uk_100g", "type": DECIMAL},
        ],
        "BioactiveCompounds": [
            {"name": "caffeine_100g", "type": DECIMAL},
            {"name": "taurine_100g", "type": DECIMAL},
            {"name": "nitrate_100g", "type": DECIMAL},
            {"name": "sulphate_100g", "type": DECIMAL},
            {"name": "carnitine_100g", "type": DECIMAL},
            {"name": "inositol_100g", "type": DECIMAL},
            {"name": "alcohol_100g", "type": DECIMAL}
        ],
        "PhysicalAndChemicalProperties": [
            {"name": "ph_100g", "type": DECIMAL},
            {"name": "glycemic-index_100g", "type": DECIMAL},
            {"name": "water-hardness_100g", "type": DECIMAL},
            {"name": "acidity_100g", "type": DECIMAL}
        ],
        "NutritionMeta": [
            {"name": "serving_size", "type": VARCHAR},
            {"name": "serving_quantity", "type": DECIMAL},
            {"name": "no_nutrition_data", "type": VARCHAR},
            {"name": "nutriscore_score", "type": DECIMAL},
            {"name": "nutriscore_grade", "type": VARCHAR},
            {"name": "nova_group", "type": DECIMAL},
            {"name": "pnns_groups_1", "type": VARCHAR},
            {"name": "pnns_groups_2", "type": VARCHAR},
            {"name": "food_groups", "type": VARCHAR},
            {"name": "food_groups_tags", "type": VARCHAR},
            {"name": "food_groups_en", "type": VARCHAR}
        ],
        "Products": [
            {"name": "code", "type": DECIMAL},
            {"name": "url", "type": VARCHAR},
            {"name": "product_name", "type": VARCHAR},
            {"name": "abbreviated_product_name", "type": VARCHAR},
            {"name": "generic_name", "type": VARCHAR},
            {"name": "brands", "type": VARCHAR},
            {"name": "brands_tags", "type": VARCHAR},
            {"name": "brand_owner", "type": VARCHAR}
        ]
    }

    

    input_path = "/Users/rui/Downloads/preprocessed_openfoodfacts.csv" # Input csv file location
    output_path = "/Users/rui/Downloads/DatabaseSystem/dataset1-split" # Output folder location

    # Load the dataset

    count = 0

    with pd.read_csv(input_path, sep='\t', low_memory=False, encoding='utf-8', on_bad_lines="skip", chunksize=10000) as reader:
        for raw_data in reader:
            count += 1
            #print("Reading chunk", count)
            data = raw_data.fillna(value = "")
            columns_food = data.columns.tolist()

            # Classify each column to its entity based on the mapping
            classified_attributes = {entity: [] for entity in entity_mapping.keys()}
            # Map columns to their respective entities
            for column in columns_food:
                for entity, attributes in entity_mapping.items():
                    attributes_list = []
                    for attribute in attributes:
                        attributes_list.append(attribute["name"])
                    if column in attributes_list:
                        classified_attributes[entity].append(column)

            #Step 2 : create separate tables for each entity and add primary key
            tables = {}
            for entity, attributes in classified_attributes.items():
                if attributes:
                    tables[entity] = data[attributes].copy()
                    # Add primary key (auto-incrementing ID)
                    tables[entity][f'{entity}ID'] = range(len(tables[entity]) * (count - 1) + 1, len(tables[entity]) * count + 1)


            # Step 3: add foreign keys
            products = tables['Products']
            # Add foreign keys to Products for related entities
            start = len(tables['Labeling']) * (count - 1) + 1
            end = len(tables['Labeling']) * count + 1
            products['LabelingID'] = range(start, end)
            products['PackagingInfoID'] = range(start, end)
            products['OriginsID'] = range(start, end)
            products['ManufacturingDetailID'] = range(start, end)
            products['ContributionHistoryID'] = range(start, end)
            products['PurchasesInfoID'] = range(start, end)
            products['AdditivesID'] = range(start, end)
            products['EnvironmentalInfoID'] = range(start, end)
            products['ImageInfoID'] = range(start, end)
            products['MiscellaneousInfoID'] = range(start, end)
            products['AllergensAndTracesID'] = range(start, end)
            products['NutritionMetaID'] = range(start, end)
            products['CategoriesID'] = range(start, end)
            products['IngredientsID'] = range(start, end)
            

            # Adding foreign keys to NutritionMeta for related entities
            start = len(tables['BioactiveCompounds']) * (count - 1) + 1
            end = len(tables['BioactiveCompounds']) * count + 1
            nutrition_meta = tables['NutritionMeta']
            nutrition_meta['BioactiveCompoundsID'] = range(start, end)
            nutrition_meta['EnergyAndFatID'] = range(start, end)
            nutrition_meta['FiberID'] = range(start, end)
            nutrition_meta['PhysicalAndChemicalPropertiesID'] = range(start, end)
            nutrition_meta['FattyInfoID'] = range(start, end)
            nutrition_meta['MineralsID'] = range(start, end)
            nutrition_meta['CarbohydratesID'] = range(start, end)
            nutrition_meta['VitaminDerivativesID'] = range(start, end)
            nutrition_meta['ProteinsID'] = range(start, end)
            nutrition_meta['VitaminsID'] = range(start, end)
            nutrition_meta['NutritionalCompositionAndScoresID'] = range(start, end)


            # Step 4: standardize and normalize the raw dataset1
            for entity in entity_mapping:
                for attribute in entity_mapping[entity]:
                    tables[entity][attribute["name"]] = tables[entity][attribute["name"]].replace("", None)


            #Step 5: save tables to csv files firstly, later should be integrated with the open food facts tables
            if False: # Not needed anymore after DB connection
                if count == 1: # Create new table for first chunk
                    for entity, table in tables.items():
                        save_path = output_path + f"/{entity}.csv"
                        table.to_csv(save_path, index=False)
                else: # Append to existing files in later chunks
                    for entity, table in tables.items():
                        save_path = output_path + f"/{entity}.csv"
                        table.to_csv(save_path, index=False, mode='a', header=False)


            # Step 6: Write SQL Queries to create the tables
            # Need to create docker DB image first: 
            # docker run --name database_project -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -e POSTGRES_DB=database_project -p 127.0.0.1:54321:5432 -d postgres

    #         types = {}
    #         for entity, attributes in entity_mapping.items():
    #             for attribute in attributes:
    #                 new_attribute = attribute["name"].replace("-", "_") # Query won't accept fields with "-"
    #                 tables[entity].rename(columns={attribute["name"]:new_attribute}, inplace=True)
    #                 types[new_attribute] = attribute["type"]

    #         if count == 1: # Create new table for first chunk
    #             if False: # Enable if only one DB should be tested
    #                 conn = psycopg2.connect(
    #                     dbname="database_project",
    #                     user='user',
    #                     password='password',
    #                     host='localhost',
    #                     port= '54321'
    #                 )
                    
    #                 conn.autocommit = True
                    
    #                 cursor = conn.cursor()

    #                 # Drop database if already exists
    #                 sql = "DROP DATABASE IF EXISTS database_project;"
    #                 cursor.execute(sql)
    #                 print("Dropped database")

    #                 # Create new database
    #                 sql = "CREATE DATABASE database_project;"
    #                 cursor.execute(sql)
    #                 print("Created database")

    #             # Create new connection directly to the new database (could maybe be done different)
    #             conn = psycopg2.connect(
    #                 dbname="database_project",
    #                 user='user',
    #                 password='password',
    #                 host='localhost',
    #                 port= '54321'
    #             )
    #             conn.autocommit = True
    #             cursor = conn.cursor()
                

    #             for entity, table in tables.items():
    #                 query = "CREATE TABLE " + entity + "("
    #                 for attribute in table:
    #                     if attribute == entity + "ID": # Primary Key
    #                         query += attribute + " INT PRIMARY KEY, "
    #                     elif attribute.endswith("ID"): # Foreign Key
    #                         foreign_key_id = attribute
    #                         foreign_key_table = foreign_key_id[:-2]
    #                         query += attribute + " INT REFERENCES " + foreign_key_table + "(" + foreign_key_id + "), "
    #                     else: # Everything else
    #                         attribute_type = types[attribute]
    #                         if attribute_type == VARCHAR:
    #                             attribute_type = "VARCHAR(10000)"
    #                         elif attribute_type == DECIMAL:
    #                             attribute_type = "DECIMAL"
    #                         query += attribute + " " + attribute_type + ", "
    #                 query = query[:-2] + ");"
    #                 match = re.search(r"CREATE TABLE (\w+)", query, re.IGNORECASE)
    #                 if match:
    #                     table_name = match.group(1)
    #                     cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")  # Drop table if it exists
    #                 cursor.execute(query) # Create table
    #                 print("Created table", entity)
            
    
    #         # Step 7: Input data into the tables
    #         engine = create_engine('postgresql://user:password@localhost:54321/database_project')
    #         for entity, table in tables.items():
    #             table.columns = [f'{col.lower()}' for col in table.columns]
    #             table.to_sql(name=entity.lower(), con=engine, if_exists='append', index=False, dtype=types)

    # conn.close()


process_dataset1() # Uncomment to test only this script
