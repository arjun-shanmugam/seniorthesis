"""
03_geocode_assessment_values.py

Geocode property assessment addresses from MassGIS.
"""
import pandas as pd

from src.utilities.dataframe_utilities import geocode_addresses

if __name__ == '__main__':
    INPUT_DATA_ASSESSMENT_VALUES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data.csv"
    INTERMEDIATE_DATA_GEOCODING = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessment_data_to_geocode"
    OUTPUT_DATA = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data_geocoded.csv"
    assessor_data = pd.read_csv(INPUT_DATA_ASSESSMENT_VALUES)

    # Geocode addresses for easy matching with eviction values.
    assessor_data = assessor_data.reset_index(drop=True)
    assessor_data.loc[:, 'STATE'] = "MA"
    columns_to_geocode = assessor_data[['SITE_ADDR', 'CITY', 'STATE', 'ZIP']]
    print(f"Sending {len(columns_to_geocode)} observations to geocoder.")
    result = geocode_addresses(columns_to_geocode,
                               INTERMEDIATE_DATA_GEOCODING)
    result.loc[:, 'id'] = result['id'].astype(int)
    result = result.set_index('id')
    print(f"Successfully geocoded {len(result) / len(assessor_data)} percent of remaining dataset. Dropping failed rows.")

    # Concatenate the geocoded data with the original assessment data.
    assessor_data = assessor_data.merge(result,
                                        right_index=True,
                                        left_index=True,
                                        how='inner',
                                        validate='1:1')

    # Rename "parsed" column.
    assessor_data = assessor_data.rename(columns={'parsed': 'full_geocoded_address'})

    assessor_data.to_csv(OUTPUT_DATA, index=False)
