import pandas as pd
import os
import numpy as np
import json

mapp = {"Person name (Last name, First name, Middle name, Full name)" : "person_name",
"Business name" : "business_name",
"Phone Number" : "phone_number",
"Address" : "address",
"Street name" : "street_name",
"City" : "city",
"Neighborhood" : "neighborhood",
"LAT/LON coordinates" : "lat_lon_cord",
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
"Other" : "other", "" : ""}

dirr = "column_output/"

df = pd.read_csv("true_labels.csv")
df["Type2"] = df["Type2"].fillna("")
df["Type3"] =  df["Type3"].fillna("")
df["Column Type"] = df["Column Type"].apply(lambda x : mapp[x])
df["Type2"] = df["Type2"].apply(lambda x : mapp[x])
df["Type3"] = df["Type3"].apply(lambda x : mapp[x])

file_dict = {}
solo_count = 0
d_count = 0
t_count = 0
for index, row in df.iterrows():
	temp = [row["Column Type"]]

	if row["Type2"] != "":
		temp.append(row["Type2"])

	if row["Type3"] != "":
		temp.append(row["Type3"])

	file_dict[row["Column Name"]] = temp

	if len(temp) == 2:
		d_count += 1
	elif len(temp) == 3:
		t_count += 1
	else:
		solo_count += 1

print ("True Type One Counts : ", solo_count)
print ("True Type Two Counts : ", d_count)
print ("True Type Three Counts : ", t_count)

output_stats = {}
for i in list(mapp.values()):
	output_stats[i] = [0,0, df[df["Column Type"] == i]["Column Type"].count() + df[df["Type2"] == i]["Type2"].count() + df[df["Type3"] == i]["Type3"].count() ]

correct = []
total = df.count()["Column Type"]

pred_counts_1 = 0
pred_counts_2 = 0
pred_counts_3 = 0
labels_output = {}
for i in os.listdir(dirr):
	with open(dirr+i, "rb") as f:
		read = json.load(f)

	dff = []
	for j in read["semantic_types"]:
		dff.append([j["semantic_type"], j["count"]])

	ll = sorted(dff, key = lambda x : x[1], reverse = True)
	temp_total = sum(item[1] for item in ll)
	
	n = len(ll)
	consider = []
	for j in range(3):
		consider.append(ll[j][0])
		if n <= j + 1:
			break
		if (ll[j][1]/temp_total - ll[j+1][1]/temp_total) > 0.3:
			break

	if len(consider) == 1:
		pred_counts_1 += 1
	elif len(consider) == 2:
		pred_counts_2 += 1
	else:
		pred_counts_3 += 1

	labels_output[i.replace(".json", "")] = consider
	out = set(file_dict[i.replace(".json", "")]).intersection(set(consider))

	if len(out) > 0:
		correct.append(i)

	for j in consider:
		output_stats[j][1] += 1

	for j in list(out):
		output_stats[j][0] += 1

# with open("predicted_labels.json", 'w') as f:
# 	json.dump(labels_output, f)

print ("Predcited Type One Counts : ", pred_counts_1)
print ("Predcited Type Two Counts : ", pred_counts_2)
print ("Predcited Type Three Counts : ", pred_counts_3)
print ("Accuracy in percent: ", round((100*len(correct))/total, 3))

# print ("Output for Labels : ", output_stats)

for i in output_stats:
	div = df[df["Column Type"] == i]["Column Type"].count() + df[df["Type2"] == i]["Type2"].count() + df[df["Type3"] == i]["Type3"].count()
	if i == "":
		continue
	if output_stats[i][1] != 0:
		print (i , " => Precision : ", round((output_stats[i][0])/output_stats[i][1],3), "   Recall : ", round((output_stats[i][0])/div, 3))
	else:
		print (i , " => Precision : ", 0.0, "   Recall : ", round((output_stats[i][0])/div, 3))