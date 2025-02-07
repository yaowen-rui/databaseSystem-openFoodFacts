import psycopg2
from psycopg2 import sql, extras

# tables from dataset1: test foreign key constraint
conn = psycopg2.connect(dbname="database_project", user="user", password="password", host="127.0.0.1", port=54321)
cursor = conn.cursor()

# somehow the foreign key constrains of CSVs-dataset1 are not added successfully during the inserting to DB
# here add them in sql
def add_foreign_key_nutritionmeta():
    foreign_keys = {
    "bioactivecompoundsid": "bioactivecompounds",
    "energyandfatid": "energyandfat",
    "fiberid": "fiber",
    "physicalandchemicalpropertiesid": "physicalandchemicalproperties",
    "fattyinfoid": "fattyinfo",
    "mineralsid": "minerals",
    "carbohydratesid": "carbohydrates",
    "vitaminderivativesid": "vitaminderivatives",
    "proteinsid": "proteins",
    "vitaminsid": "vitamins",
    "nutritionalcompositionandscoresid": "nutritionalcompositionandscores"
    }

    try:
        # Add missing columns to nutritionmeta
        for column, parent_table in foreign_keys.items():
            alter_query = f"ALTER TABLE nutritionmeta ADD COLUMN {column} INT;"
            cursor.execute(alter_query)
            print(f"Added column: {column}")

        # populate foreign key columns with primary key values
        for column, parent_table in foreign_keys.items():
            print(f"populating column: {column}")
            update_query = f"""
            UPDATE nutritionmeta
            SET {column} = nutritionmetaid
            WHERE {column} IS NULL;
            """
            cursor.execute(update_query)
            print(f"populated column: {column} with values")

        # Add foreign key constraints
        for column, parent_table in foreign_keys.items():
            constraint_query = f"""
            ALTER TABLE nutritionmeta
            ADD CONSTRAINT fk_nutritionmeta_{column}
            FOREIGN KEY ({column})
            REFERENCES {parent_table}({column})
            ON DELETE CASCADE;
            """
            cursor.execute(constraint_query)
            print(f"Added foreign key: {column} -> {parent_table}({column})")

        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
add_foreign_key_nutritionmeta()


def add_foreign_key_products():
    foreign_keys = {
    "labelingid": "labeling",
    "packaginginfoid": "packaginginfo",
    "originsid": "origins",
    "manufacturingdetailid": "manufacturingdetail",
    "contributionhistoryid": "contributionhistory",
    "purchasesinfoid": "purchasesinfo",
    "additivesid": "additives",
    "environmentalinfoid": "environmentalinfo",
    "imageinfoid": "imageinfo",
    "miscellaneousinfoid": "miscellaneousinfo",
    "allergensandtracesid": "allergensandtraces",
    "nutritionmetaid": "nutritionmeta",
    "categoriesid": "categories",
    "ingredientsid": "ingredients"
    }

    try:
        # Step 1: Add missing columns to products
        for column, parent_table in foreign_keys.items():
            alter_query = f"ALTER TABLE products ADD COLUMN {column} INT;"
            cursor.execute(alter_query)
            print(f"Added column: {column}")

        # populate foreign key columns with primary key values
        for column, parent_table in foreign_keys.items():
            print(f"populating column: {column}")
            update_query = f"""
            UPDATE products
            SET {column} = productsid
            WHERE {column} IS NULL;
            """
            cursor.execute(update_query)
            print(f"populated column: {column} with values")

        # Step 3: Add foreign key constraints
        for column, parent_table in foreign_keys.items():
            constraint_query = f"""
            ALTER TABLE products
            ADD CONSTRAINT fk_products_{column}
            FOREIGN KEY ({column})
            REFERENCES {parent_table}({column})
            ON DELETE CASCADE;
            """
            cursor.execute(constraint_query)
            print(f"Added foreign key: {column} -> {parent_table}({column})")

        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

#add_foreign_key_products()


