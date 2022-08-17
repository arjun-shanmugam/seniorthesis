"""
clean_assessment_values.py

Cleans property assessment values from MassGIS.
"""
import geopandas

gdf = geopandas.read_file("/Users/arjunshanmugam/Downloads/L3_SHP_M001_Abington/M001Assess_CY22_FY22.dbf")
gdf2 = geopandas.read_file("/Users/arjunshanmugam/Downloads/AllParcelData_SHP_20220810/L3_SHP_M001_Abington_CY22_FY22/M001TaxPar_CY22_FY22.shp")
print(gdf2.columns)
print(gdf.columns)