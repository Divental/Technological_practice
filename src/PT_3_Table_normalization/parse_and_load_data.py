import os
import json

from logger.logger_config import setup_logger
from execute_sql_query import fill_continent_table, fill_country_table, fill_city_table

logger = setup_logger(__name__)


def parse_data_from_json_file() -> dict | None:
    """
    Reads and parses JSON data from a file named 'country.json' located in the script directory.

    This function:
    1. Determines the absolute path to 'country.json' based on the location of the current script.
    2. Opens the JSON file with UTF-8 encoding.
    3. Parses the JSON content into a Python dictionary.
    4. Logs success or errors during the operation.
    5. Returns the parsed data if successful, otherwise returns None.

    Returns:
        dict | None: Parsed JSON data as a dictionary if successful; None if an error occurs.

    Raises:
        None: Exceptions are caught and logged within the function.

    Logging:
        - Logs a success message when data is read successfully.
        - Logs exceptions for file not found, JSON decoding errors, and missing keys.

    Notes:
        - The function expects 'country.json' to be located in the same directory as this script.
        - If any error occurs, the function returns None.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(script_dir, "country.json")

    try:
        with open(json_path, "r", encoding='utf-8') as file:
            json_data = json.load(file)

        logger.info("Successfully read data")
        return json_data
    
    except FileNotFoundError:
        logger.exception("File not found")
    except json.JSONDecodeError:
        logger.exception("Encoding error JSON")
    except KeyError as e:
        logger.exception(f"No key: {e}")


def normalize_json_file() -> None:
    """
    Parses hierarchical JSON data from 'country.json' and inserts it into the database.

    The function:
    - Loads JSON using `parse_data_from_json_file()`.
    - Inserts continent, country, and city data using corresponding `fill_*_table()` functions.
    - Assumes a specific JSON structure with continents, countries, and cities.

    Returns:
        str: Success message upon completion.

    Raises:
        KeyError, TypeError: If JSON structure is invalid.
        Exceptions from helper functions may also propagate.
    """
    json_data = parse_data_from_json_file()

    for continent_block in json_data:

        continent_name = continent_block["continent"]
        continent_id = fill_continent_table(continent_name)
        
        for country in continent_block["countries"]:
            
            name = country["country"]
            capital = country["capital"]
            area = country["area"]
            population = country["population"]

            country_id = fill_country_table(continent_id, name, capital, area, population)

            for city in country["largest_cities"]:
                fill_city_table(country_id, city)
        
    logger.info("Successfully written data")

