import json
import pandas as pd

mapp = {"Person name (Last name, First name, Middle name, Full name)" : "person_name",
"Business name" : "business_name",
"Phone Number" : "phone_number",
"Address" : "address",
"Street name" : "street_name",
"City" : "city",
"Neighborhood" : "neighborhood",
"LAT/LON coordinates" : "lat_lon_coord",
"Zip code" : "zip_code",
"Borough" : "borough",
"School name (Abbreviations and full names)" : "school_name",
"Color" : "color",
"Car make" : "car_make",
"City agency (Abbreviations and full names)" : "city_agency",
"Areas of study (e.g., Architecture, Animal Science, Communications)" : "area_of_study",
"Subjects in school (e.g., MATH A, MATH B, US HISTORY)" : "subject_in_school",
"School Levels (K-2, ELEMENTARY, ELEMENTARY SCHOOL, MIDDLE)" : "school_level",
"Websites (e.g., ASESCHOLARS.ORG)" : "website",
"Building Classification (e.g., R0-CONDOMINIUM, R2-WALK-UP)" : "building_classification",
"Vehicle Type (e.g., AMBULANCE, VAN, TAXI, BUS)" : "vehicle_type",
"Type of location (e.g., ABANDONED BUILDING, AIRPORT TERMINAL, BANK," : "location_type",
"Parks/Playgrounds (e.g., CLOVE LAKES PARK, GREENE PLAYGROUND)" : "park_playground",
"Other" : "other", 
"" : ""}


df = pd.read_csv("true_labels.csv")
df["Type2"] = df["Type2"].fillna("")
df["Type3"] =  df["Type3"].fillna("")
df["Column Type"] = df["Column Type"].apply(lambda x : mapp[x])
df["Type2"] = df["Type2"].apply(lambda x : mapp[x])
df["Type3"] = df["Type3"].apply(lambda x : mapp[x])

out = {"actual_types" : []}
for index, row in df.iterrows():
	temp = {"column_name" : row["Column Name"], "manual_labels" : [row["Column Type"]]}

	if row["Type2"] != "":
		temp["manual_labels"].append(row["Type2"])

	if row["Type3"] != "":
		temp["manual_labels"].append(row["Type3"])

	out["actual_types"].append(temp)

with open("task2-manual-labels.json ", "w") as f:
	json.dump(out, f)