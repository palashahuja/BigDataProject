import json
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.naive_bayes import MultinomialNB
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from functools import reduce
import time
import numpy as np
import re
import os
import random
file_names = ['cvh6-nmyi.SCHOOL.txt.gz',
 'bbs3-q5us.ZIP.txt.gz',
 'kiv2-tbus.Vehicle_Color.txt.gz',
 'sqcr-6mww.Location.txt.gz',
 'vrn4-2abs.SCHOOL_LEVEL_.txt.gz',
 's27g-2w3u.School_Name.txt.gz',
 'jt7v-77mi.Vehicle_Make.txt.gz',
 '735p-zed8.CANDMI.txt.gz',
 '7btz-mnc8.Provider_First_Name.txt.gz',
 'niy5-4j7q.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
 'vw9i-7mzq.interest4.txt.gz',
 '2sps-j9st.PERSON_LAST_NAME.txt.gz',
 'uzcy-9puk.Incident_Zip.txt.gz',
 'f3cg-u8bv.Agency.txt.gz',
 'mdcw-n682.First_Name.txt.gz',
 '6anw-twe4.FirstName.txt.gz',
 'crc3-tcnm.BOROUGH.txt.gz',
 '2bmr-jdsv.DBA.txt.gz',
 '4twk-9yq2.CrossStreet2.txt.gz',
 'pqg4-dm6b.Address1.txt.gz',
 'hy4q-igkk.Cross_Street_1.txt.gz',
 'myei-c3fa.Neighborhood.txt.gz',
 'weg5-33pj.SCHOOL_LEVEL_.txt.gz',
 'gez6-674h.BORO.txt.gz',
 'h9gi-nx95.VEHICLE_TYPE_CODE_1.txt.gz',
 'ipu4-2q9a.Site_Safety_Mgr_s_First_Name.txt.gz',
 's9d3-x4fz.ZIP.txt.gz',
 'tg3t-nh4h.BusinessName.txt.gz',
 'yrf7-4wry.COMPARABLE_RENTAL___2___Neighborhood.txt.gz',
 '4hiy-398i.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
 'qgea-i56i.PREM_TYP_DESC.txt.gz',
 'kj4p-ruqc.StreetName.txt.gz',
 'faiq-9dfq.Vehicle_Body_Type.txt.gz',
 'fbaw-uq4e.Location_1.txt.gz',
 '7yay-m4ae.AGENCY_NAME.txt.gz',
 '2v9c-2k7f.DBA.txt.gz',
 'dtmw-avzj.BOROUGH.txt.gz',
 'bty7-2jhb.Site_Safety_Mgr_s_Last_Name.txt.gz',
 'va74-3m6c.company_phone.txt.gz',
 'uzcy-9puk.Street_Name.txt.gz',
 'q5za-zqz7.Agency.txt.gz',
 '8k4x-9mp5.Last_Name__only_2014_15_.txt.gz',
 'jz4z-kudi.Violation_Location__City_.txt.gz',
 'sqcr-6mww.Park_Facility_Name.txt.gz',
 '72ss-25qh.Borough.txt.gz',
 't8hj-ruu2.First_Name.txt.gz',
 '8eux-rfe8.INPUT_1_Borough.txt.gz',
 'erm2-nwe9.Park_Facility_Name.txt.gz',
 '3btx-p4av.COMPARABLE_RENTAL___1__Building_Classification.txt.gz',
 'sv2w-rv3k.BORO.txt.gz',
 '4qii-5cz9.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
 'i8ys-e4pm.CORE_COURSE_9_12_ONLY_.txt.gz',
 'dm9a-ab7w.STREET_NAME.txt.gz',
 'kwmq-dbub.CITY.txt.gz',
 '9b9u-8989.DBA.txt.gz',
 'vhah-kvpj.Borough.txt.gz',
 'cvh6-nmyi.SCHOOL_LEVEL_.txt.gz',
 '4n2j-ut8i.SCHOOL_LEVEL_.txt.gz',
 '3aka-ggej.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
 'm59i-mqex.QUEENS_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz',
 'rmv8-86p4.BROOKLYN_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz',
 'mdcw-n682.Middle_Initial.txt.gz',
 'mdcw-n682.Agency_Website.txt.gz',
 'myei-c3fa.Neighborhood_2.txt.gz',
 'cvse-perd.Agency_Name.txt.gz',
 'ruce-cnp6.Agency.txt.gz',
 'e9xc-u3ds.ZIP.txt.gz',
 'fb26-34vu.CORE_SUBJECT__MS_CORE_and_9_12_ONLY_.txt.gz',
 '5uac-w243.PREM_TYP_DESC.txt.gz',
 'ci93-uc8s.Vendor_DBA.txt.gz',
 'bs8b-p36w.LOCATION.txt.gz',
 'n3p6-zve2.website.txt.gz',
 '9z9b-6hvk.Borough.txt.gz',
 'ptev-4hud.City.txt.gz',
 'yayv-apxh.SCHOOL_LEVEL_.txt.gz',
 'yjub-udmw.Location__Lat__Long_.txt.gz',
 'ic3t-wcy2.Applicant_s_First_Name.txt.gz',
 'aiww-p3af.Location.txt.gz',
 'wks3-66bn.School_Name.txt.gz',
 '3rfa-3xsf.Street_Name.txt.gz',
 'n2mv-q2ia.Address_1__self_reported_.txt.gz',
 '2sps-j9st.PERSON_FIRST_NAME.txt.gz',
 'cspg-yi7g.PHONE.txt.gz',
 'mu46-p9is.Location.txt.gz',
 'pdpg-nn8i.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz',
 'uzcy-9puk.School_Name.txt.gz',
 'pqg4-dm6b.Phone.txt.gz',
 'd3ge-anaz.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz',
 'n2s5-fumm.BRONX_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
 '3btx-p4av.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
 'urc8-8bjy.BORO.txt.gz',
 'aiww-p3af.Incident_Zip.txt.gz',
 'ahjc-fdu3.PRINCIPAL_PHONE_NUMBER.txt.gz',
 'r4c5-ndkx._Phone.txt.gz',
 'crc3-tcnm.CORE_SUBJECT.txt.gz',
 'i8ys-e4pm.CORE_SUBJECT_9_12_ONLY_.txt.gz',
 'qcdj-rwhu.BUSINESS_NAME2.txt.gz',
 'jhjm-vsp8.Agency.txt.gz',
 's3k6-pzi2.interest5.txt.gz',
 'erm2-nwe9.Landmark.txt.gz',
 '3miu-myq2.COMPARABLE_RENTAL___2__Neighborhood.txt.gz',
 'ccgt-mp8e.Borough.txt.gz',
 'kiv2-tbus.Vehicle_Make.txt.gz',
 'brga-xeqy.Agency.txt.gz',
 'qpm9-j523.org_neighborhood.txt.gz',
 'mq9d-au8i.School_Name.txt.gz',
 'uq7m-95z8.interest1.txt.gz',
 'sxmw-f24h.Cross_Street_1.txt.gz',
 'i6b5-j7bu.TOSTREETNAME.txt.gz',
 'vw9i-7mzq.interest1.txt.gz',
 'dpm2-m9mq.owner_city.txt.gz',
 'vwxi-2r5k.CORE_SUBJECT.txt.gz',
 '8gpu-s594.SCHOOL_NAME.txt.gz',
 'mdcw-n682.Last_Name.txt.gz',
 '4pt5-3vv4.Location.txt.gz',
 '8586-3zfm.School_Name.txt.gz',
 'iz2q-9x8d.Owner_s__Phone__.txt.gz',
 'bjuu-44hx.DVV_MAKE.txt.gz',
 'ph7v-u5f3.WEBSITE.txt.gz',
 'hy4q-igkk.Park_Facility_Name.txt.gz',
 '735p-zed8.CITY.txt.gz',
 'w9ak-ipjd.Applicant_Last_Name.txt.gz',
 '922w-z7da.address_1.txt.gz',
 'ph7v-u5f3.TOP_VEHICLE_MODELS___5.txt.gz',
 'mjux-q9d4.SCHOOL.txt.gz',
 'ebkm-iyma.Street_Address.txt.gz',
 'vr8p-8shw.DVT_MAKE.txt.gz',
 'ci93-uc8s.telephone.txt.gz',
 'c284-tqph.Vehicle_Color.txt.gz',
 'vwxi-2r5k.BOROUGH.txt.gz',
 'w9if-3pyn.School_Name.txt.gz',
 '956m-xy24.COMPARABLE_RENTAL_2__Building_Classification.txt.gz',
 'rtws-c2ai.School_Name.txt.gz',
 'dm9a-ab7w.AUTH_REP_LAST_NAME.txt.gz',
 'y4fw-iqfr.Address.txt.gz',
 'jz4z-kudi.Respondent_Address__Zip_Code_.txt.gz',
 's3zn-tf7c.QUEENS_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
 '9ck8-hj3u.PRINCIPAL_PHONE_NUMBER.txt.gz',
 'jt7v-77mi.Vehicle_Color.txt.gz',
 'niy5-4j7q.COMPARABLE_RENTAL___2__Building_Classification.txt.gz',
 '5mw2-hzqx.BROOKLYN_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz',
 '6rrm-vxj9.parkname.txt.gz',
 '5cd6-v74i.School_Name.txt.gz',
 'urzf-q2g5.Phone_Number.txt.gz',
 'ci93-uc8s.Website.txt.gz',
 's3k6-pzi2.interest1.txt.gz',
 'vx8i-nprf.MI.txt.gz',
 'cgz5-877h.SCHOOL_LEVEL_.txt.gz',
 'sxmw-f24h.Street_Name.txt.gz',
 'f42p-xqaa.BROOKLYN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
 '3rfa-3xsf.Cross_Street_2.txt.gz',
 'uqxv-h2se.independentwebsite.txt.gz',
 'pdk7-puui.Agency.txt.gz',
 'upwt-zvh3.SCHOOL_LEVEL_.txt.gz',
 '8u86-bviy.Address_1__self_reported_.txt.gz',
 'emnd-d8ba.PRINCIPAL_PHONE_NUMBER.txt.gz',
 't8hj-ruu2.Business_Phone_Number.txt.gz',
 'vw9i-7mzq.interest3.txt.gz',
 '6anw-twe4.LastName.txt.gz',
 'uchs-jqh4.School_Name.txt.gz',
 'uq7m-95z8.neighborhood.txt.gz',
 '52dp-yji6.Owner_First_Name.txt.gz',
 's79c-jgrm.City.txt.gz',
 'ydkf-mpxb.CrossStreetName.txt.gz',
 'w9ak-ipjd.City.txt.gz',
 'pvqr-7yc4.Vehicle_Make.txt.gz',
 'qusa-igsv.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
 'a5qt-5jpu.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
 'p937-wjvj.LOCATION.txt.gz',
 '4kpn-sezh.Website.txt.gz',
 'ic3t-wcy2.Zip.txt.gz',
 'yahh-6yjc.School_Type.txt.gz',
 'en2c-j6tw.BRONX_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz',
 '7btz-mnc8.Provider_Last_Name.txt.gz',
 'qu8g-sxqf.First_Name.txt.gz',
 '4d7f-74pe.Agency.txt.gz',
 'a2pm-dj2w.Borough.txt.gz',
 'jz4z-kudi.Respondent_Address__City_.txt.gz',
 'as69-ew8f.TruckMake.txt.gz',
 'sxx4-xhzg.Park_Site_Name.txt.gz',
 '6bgk-3dad.RESPONDENT_CITY.txt.gz',
 'dm9a-ab7w.AUTH_REP_FIRST_NAME.txt.gz',
 'ipu4-2q9a.Site_Safety_Mgr_s_Last_Name.txt.gz',
 '2bnn-yakx.Vehicle_Body_Type.txt.gz',
 'cyfw-hfqk.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
 '2bnn-yakx.Vehicle_Make.txt.gz',
 'a9md-ynri.MI.txt.gz',
 'w9ak-ipjd.Owner_s_Business_Name.txt.gz',
 '72ss-25qh.Agency_ID.txt.gz',
 'tqtj-sjs8.FromStreetName.txt.gz',
 'k3cd-yu9d.CANDMI.txt.gz',
 'xck4-5xd5.website.txt.gz',
 'xubg-57si.OWNER_BUS_STREET_NAME.txt.gz',
 'hy4q-igkk.Intersection_Street_2.txt.gz',
 's3k6-pzi2.website.txt.gz',
 '6je4-4x7e.SCHOOL_LEVEL_.txt.gz',
 'dm9a-ab7w.APPLICANT_FIRST_NAME.txt.gz',
 'h9gi-nx95.VEHICLE_TYPE_CODE_5.txt.gz',
 'uq7m-95z8.interest6.txt.gz',
 'mqdy-gu73.Color.txt.gz',
 'fxdy-q85h.Agency.txt.gz',
 'aiww-p3af.Street_Name.txt.gz',
 '7jkp-5w5g.Agency.txt.gz',
 '446w-773i.Address_1.txt.gz',
 'pq5i-thsu.DVC_MAKE.txt.gz',
 '4s7y-vm5x.CORE_SUBJECT.txt.gz',
 '3btx-p4av.COMPARABLE_RENTAL___1__Neighborhood.txt.gz',
 'h9gi-nx95.VEHICLE_TYPE_CODE_3.txt.gz',
 '3rfa-3xsf.Incident_Zip.txt.gz',
 'yg5a-hytu.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
 'kiv2-tbus.Vehicle_Body_Type.txt.gz',
 'i9pf-sj7c.INTEREST.txt.gz',
 'ffnc-f3aa.SCHOOL_LEVEL_.txt.gz',
 'uwyv-629c.StreetName.txt.gz',
 '2bnn-yakx.Vehicle_Color.txt.gz',
 'ipu4-2q9a.Owner_s_House_Zip_Code.txt.gz',
 'sxmw-f24h.School_Name.txt.gz',
 'dj4e-3xrn.SCHOOL_LEVEL_.txt.gz',
 'yvxd-uipr.Location_1.txt.gz',
 'nhms-9u6g.Name__Last__First_.txt.gz',
 'ge8j-uqbf.interest.txt.gz',
 'niy5-4j7q.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Neighborhood.txt.gz',
 'c284-tqph.Vehicle_Make.txt.gz',
 'w9ak-ipjd.Filing_Representative_First_Name.txt.gz',
 'h9gi-nx95.VEHICLE_TYPE_CODE_4.txt.gz',
 '52dp-yji6.Owner_Last_Name.txt.gz',
 '6wcu-cfa3.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz',
 'ajxm-kzmj.NeighborhoodName.txt.gz',
 '43nn-pn8j.DBA.txt.gz',
 'wcmg-48ep.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
 'ji82-xba5.address.txt.gz',
 'cspg-yi7g.ADDRESS.txt.gz',
 'mrxb-9w9v.BOROUGH___COMMUNITY.txt.gz',
 'a5td-mswe.Vehicle_Color.txt.gz',
 'jz4z-kudi.Violation_Location__Zip_Code_.txt.gz',
 'kwmq-dbub.CANDMI.txt.gz',
 'qu8g-sxqf.MI.txt.gz',
 't8hj-ruu2.License_Business_City.txt.gz',
 'e9xc-u3ds.CANDMI.txt.gz',
 '8gqz-6v9v.Website.txt.gz',
 'bdjm-n7q4.CrossStreet2.txt.gz',
 'fzv4-jan3.SCHOOL_LEVEL_.txt.gz',
 '3rfa-3xsf.Park_Facility_Name.txt.gz',
 'faiq-9dfq.Vehicle_Color.txt.gz',
 'as69-ew8f.StartCity.txt.gz',
 'erm2-nwe9.Incident_Zip.txt.gz',
 's3k6-pzi2.interest4.txt.gz',
 'bdjm-n7q4.Location.txt.gz',
 'pvqr-7yc4.Vehicle_Color.txt.gz',
 'mjux-q9d4.SCHOOL_LEVEL_.txt.gz',
 'szkz-syh6.Prequalified_Vendor_Address.txt.gz',
 'aiww-p3af.School_Name.txt.gz',
 '9b9u-8989.Establishment_Street.txt.gz',
 'h9gi-nx95.VEHICLE_TYPE_CODE_2.txt.gz',
 'xne4-4v8f.SCHOOL_LEVEL_.txt.gz',
 'f4rp-2kvy.Street.txt.gz',
 'pvqr-7yc4.Vehicle_Body_Type.txt.gz',
 'w6yt-hctp.BROOKLYN_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
 '4e2n-s75z.Address.txt.gz',
 'n84m-kx4j.VEHICLE_MAKE.txt.gz',
 'sqcr-6mww.Cross_Street_2.txt.gz',
 'p937-wjvj.HOUSE_NUMBER.txt.gz',
 'uzcy-9puk.Intersection_Street_2.txt.gz',
 '5fn4-dr26.City.txt.gz']



regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)



SIGN = '[\+-]?'
DECIMALS = '(\.[0-9]+)?'
ZEROS = '(\.0+)?'

LATITUDE =  f'{SIGN}(90{ZEROS}|[1-8]\d{DECIMALS}|\d{DECIMALS})'
LONGITUDE = f'{SIGN}(180{ZEROS}|1[0-7]\d{DECIMALS}|[1-9]\d{DECIMALS}|\d{DECIMALS})'

LAT_LONG_REGEX = f'\({LATITUDE},\s*{LONGITUDE}\)'

all_regex = {
    "phone_number"          :  "^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}",
    "zip_code"              :  "^\d{5}(-\d{4})?$",
    "lat_lon_cord"          :  LAT_LONG_REGEX
}

for reg_exp in all_regex:
    all_regex[reg_exp] = re.compile(all_regex[reg_exp])

all_regex['website'] = re.compile("^(?:http(s)?:\/\/)?[\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:/?#[\]@!\$&'\(\)\*\+,;=.]+$", re.IGNORECASE)
all_regex['website_inter'] = re.compile('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', re.IGNORECASE)

# load training data and test data
all_train_data = json.load(open('all_train_data.json', 'r'))


train_data = []
for key in all_train_data.keys():
    train_data.extend([[x, key] for x in all_train_data[key]])
random.shuffle(train_data)


train_df = pd.DataFrame(train_data, columns=['input', 'label'])
vectorizer = CountVectorizer(analyzer='char_wb', ngram_range=(3,7)).fit(train_df['input'])
encoder = OrdinalEncoder().fit(np.concatenate((train_df['label'].values, np.array(['other'], dtype='object'))).reshape(-1,1))

word_mat = vectorizer.transform(train_df['input'])
y = encoder.transform(train_df['label'].values.reshape(-1,1))
clf = MultinomialNB()
clf.fit(word_mat, y)

def get_match_type(x):
    list_of_keys = ['phone_number', 'zip_code', 'lat_lon_cord', 'website']
    for key in list_of_keys:
        if all_regex[key].match(str(x)):
            return key
    if len(all_regex['website_inter'].findall(x)) > 0:
        return 'website'
    return 'null'

@f.pandas_udf(StringType())
def get_semantic_type(x):
    return x.apply(lambda y : get_match_type(y))


@f.pandas_udf(StringType())
def get_pred_type(x):
    return pd.Series(encoder.inverse_transform(clf.predict(vectorizer.transform(x)).reshape(-1,1)).flatten())

def get_prediction_rule(x):
    if x is None:
        return 'null'
    if x == '-':
        return 'other'
    if 'PARK' in x:
        return 'park_playground'
    if 'INC' in x or 'LLC' in x:
        return 'business_name'
    if 'SCHOOL' in x or 'ACADEMY' in x or 'CAMPUS' in x:
        return 'school_name' 
    return 'null'

@f.pandas_udf(StringType())
def get_rule_based(x):
    return x.apply(lambda y : get_prediction_rule(y))


# remove nulls
def remove_null(x):
    if x is None:
        return 'null'
    print(x)
    return str(x)
    
@f.pandas_udf(StringType())
def get_no_null_values(x):
    return x.apply(lambda y: remove_null(y))


session_name = 'spark_task_2_proj'
spark = SparkSession.builder.appName(session_name).getOrCreate()


list_of_all_datasets = []
for file in os.listdir('./datasets_file/'):
    if file.endswith('.csv'):
        cur_key = file.replace('.csv', '')
        #print(cur_key)
        final_path  = 'file://' + os.path.abspath('.') + '/datasets_file/' + file
        print(final_path)
        dataset = spark.read.format('csv').options(header='true',inferschema='false',multiLine='true', delimiter=',', encoding = "ISO-8859-1").load(final_path)
        dataset = dataset.select(f.col(dataset.columns[0]).alias('column_value'))
        dataset = dataset.withColumn('soundex_phon_cur', f.soundex(f.col(dataset.columns[0])))
        dataset = dataset.withColumn('column_name', f.lit(cur_key).cast(StringType()))
        list_of_all_datasets.append(dataset)

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

merged_df = unionAll(*list_of_all_datasets)

start_time = time.time()
# file_names = ['dtmw-avzj.BOROUGH.txt.gz']
for file in file_names:
    cur_dataset = spark.read.format('csv').options(header='false',inferschema='false',multiLine='true', delimiter='\t', encoding = "ISO-8859-1").load('/user/hm74/NYCColumns/' + file)
    print(file)
    print(cur_dataset.count())
    cur_dataset = cur_dataset.withColumn('_c0', get_no_null_values(f.col('_c0')))
    cur_dataset = cur_dataset.withColumn('sem_type', get_semantic_type(f.col('_c0')))
    # rule based mechanism
    cur_dataset  = cur_dataset.withColumn('sem_type', f.when(f.col('sem_type') == 'null', get_rule_based(f.col('_c0'))).otherwise(f.col('sem_type')))
    # based on soundex and edit distance
    cur_dataset = cur_dataset.withColumn('soundex_phon', f.when(f.col('sem_type') == 'null', f.soundex(f.col('_c0'))).otherwise(f.lit('null').cast(StringType())))
    cur_dataset = cur_dataset.join(merged_df, [f.col('soundex_phon_cur') == f.col('soundex_phon')], 'left_outer').withColumn('edit_dist', f.levenshtein(f.col('column_value'), f.col('_c0')))
    min_dataset = cur_dataset.groupBy('_c0').agg(f.min(f.col('edit_dist')).alias('min_edit_dist')).filter(f.col('min_edit_dist')<=3).withColumnRenamed('_c0', 'c_value')
    temp_dataset = cur_dataset.join(min_dataset, [cur_dataset._c0 == min_dataset.c_value], 'left_outer').filter(cur_dataset.edit_dist == min_dataset.min_edit_dist) 
    temp_dataset = temp_dataset.select(f.col('_c0').alias('c_value'), f.col('column_value').alias('col_value'), f.col('column_name').alias('col_name'), 'min_edit_dist')
    temp_dataset = temp_dataset.groupBy(f.col('c_value')).agg(f.first(f.col('col_name')).alias('col_name'))
    cur_dataset  = cur_dataset.join(temp_dataset, [cur_dataset._c0 == temp_dataset.c_value], 'left_outer').withColumn('sem_type', f.when(f.col('col_name') != 'null', f.col('col_name')).otherwise(f.col('sem_type')))
    cur_dataset  = cur_dataset.select('_c0', '_c1', 'sem_type').distinct()
    # prediction with sklearn
    cur_dataset  = cur_dataset.withColumn('sem_type', f.when(f.col('sem_type') == 'null', get_pred_type(f.col('_c0'))).otherwise(f.col('sem_type')))
    cur_group_op = cur_dataset.groupBy(f.col('sem_type')).agg(f.sum(f.col('_c1')).alias('count'))
    cur_df       = cur_group_op.toPandas().to_dict()
    cur_key      = file.replace('.txt.gz', '')
    output_df    = {}
    output_df["column_name"] = cur_key
    output_df["semantic_types"] = []
    for key in cur_df['sem_type']:
        new_dict = {}
        new_dict["semantic_type"] = cur_df['sem_type'][key]
        new_dict["count"] = int(cur_df['count'][key])
        output_df["semantic_types"].append(new_dict)
#         output_df[cur_df['sem_type'][key]] = curdf['count'][key]
    with open('./column_output/' + cur_key + '.json', 'w+') as new_file:
        json.dump(output_df, new_file, indent=4)
end_time = time.time()
print(end_time - start_time)
