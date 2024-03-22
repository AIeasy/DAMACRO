
import warnings
import joblib
import time
import numpy as np
from sklearn.preprocessing import LabelEncoder
import pandas as pd
import random
from collections import Counter
from numba import njit
def incremental_k_prototypes(chunk, n_clus, cat_index, num_index, first_chunk, centroids):
    if first_chunk:
        samples = random.sample(range(len(chunk)), n_clus)
        for i in range(len(samples)):
            centroids.append(chunk[samples[i]])
    temp = np.array(chunk)
    gamma = 0.5 * np.mean(np.array(temp[:, num_index], dtype=float).std(axis=0))
    labels = []
    for i in range(len(chunk)):
        cost_list = cost_function(chunk[i], centroids, gamma, cat_index, num_index)
        #print(cost_list)
        labels.append(np.argmin(cost_list))
    update_centroids(n_clus, centroids, chunk, labels, cat_index, num_index)
    return labels, centroids


def cost_function(data, centroids, gamma, cat_index, num_index):
    cost_list = []
    for i in range(len(centroids)):
        distance = 0
        for j in range(len(num_index)):
            centroids_num = float(centroids[i][num_index[j]])
            data_num = float(data[num_index[j]])
            distance += calculate_distance(centroids_num, data_num)
        similarity = 0
        for j in range(len(cat_index)):
            if centroids[i][cat_index[j]] == data[cat_index[j]]:
                similarity += 1
        cost = distance + gamma * similarity
        cost_list.append(cost)
    return np.array(cost_list)


@njit
def calculate_distance(centroid_num, data_num):
    return (centroid_num - data_num)**2


def update_centroids(n_clus, centroids, data, clusters, cat_index, num_index):
    count = [clusters.count(i) for i in range(n_clus)]

    for i in range(len(num_index)):
        sum = np.zeros(n_clus)
        for j in range(len(data)):
            numeric_value = float(data[j][num_index[i]])
            sum[clusters[j]] += numeric_value

        sum_clean = np.nan_to_num(sum, nan=0, posinf=np.finfo(np.float64).max, neginf=np.finfo(np.float64).min)
        count_clean = np.nan_to_num(count, nan=1, posinf=np.finfo(np.float64).max, neginf=np.finfo(np.float64).min)
        with np.errstate(divide='ignore', invalid='ignore'):
            mean = np.where(count_clean != 0, np.divide(sum_clean, count_clean), 0)

        for j in range(n_clus):
            centroids[j][num_index[i]] = float(mean[j])
    for i in range(len(cat_index)):
        cat = [[] for _ in range(n_clus)]
        for j in range(len(data)):
            cat[clusters[j]].append(data[j][cat_index[i]])
        for j in range(n_clus):
            if cat[j]:
                centroids[j][cat_index[i]] = list(Counter(cat[j]).keys())[0]
            else:
                centroids[j][cat_index[i]] = 0

def preprocess_data_km(chunk):

    label_encoder = LabelEncoder()

# Iterate through each column in the DataFrame
    '''
    for column in chunk.columns:
        # Check if the column is of object type and has 10 or fewer unique values
        if chunk[column].dtype == 'object' and chunk[column].nunique() <= 10:
            # Apply Label Encoding on this column
            chunk[column] = label_encoder.fit_transform(chunk[column])
    '''
    numeric_columns = chunk.select_dtypes(include='number')
    
    return numeric_columns
def preprocess_flight(raw_data):
    flight_num_indices = [10, 11, 12, 13]
    flight_cat_indices = [2, 3, 4, 5, 6, 7, 8, 16, 17, 18, 19]

    flight_date_dict = {
    '2012-11-01': 1,  '2012-11-02': 2,  '2012-11-03': 3,  '2012-11-04': 4,
    '2012-11-05': 5,  '2012-11-06': 6,  '2012-11-07': 7,  '2012-11-08': 8,
    '2012-11-09': 9,  '2012-11-10': 10, '2012-11-11': 11, '2012-11-12': 12,
    '2012-11-13': 13, '2012-11-14': 14, '2012-11-15': 15, '2012-11-16': 16,
    '2012-11-17': 17, '2012-11-18': 18, '2012-11-19': 19, '2012-11-20': 20,
    '2012-11-21': 21, '2012-11-22': 22, '2012-11-23': 23, '2012-11-24': 24,
    '2012-11-25': 25, '2012-11-26': 26, '2012-11-27': 27, '2012-11-28': 28,
    '2012-11-29': 29, '2012-11-30': 30, '2012-12-01': 31, '2012-12-02': 32,
    '2012-12-03': 33, '2012-12-04': 34, '2012-12-05': 35, '2012-12-06': 36,
    '2012-12-07': 37, '2012-12-08': 38, '2012-12-09': 39, '2012-12-10': 40,
    '2012-12-11': 41, '2012-12-12': 42, '2012-12-13': 43, '2012-12-14': 44,
    '2012-12-15': 45, '2012-12-16': 46, '2012-12-17': 47, '2012-12-18': 48,
    '2012-12-19': 49, '2012-12-20': 50, '2012-12-21': 51, '2012-12-22': 52,
    '2012-12-23': 53, '2012-12-24': 54, '2012-12-25': 55, '2012-12-26': 56,
    '2012-12-27': 57, '2012-12-28': 58, '2012-12-29': 59, '2012-12-30': 60,
    '2012-12-31': 61
    }

    carrier_dict = {
        'AA': 1,
        'AS': 2,
        'B6': 3,
        'DL': 4,
        'EV': 5,
        'F9': 6,
        'FL': 7,
        'HA': 8,
        'MQ': 9,
        'OO': 10,
        'UA': 11,
        'US': 12,
        'VX': 13,
        'WN': 14,
        'YV': 15
    }

    origin_state_dict = {
        'AK': 1,
        'AL': 2,
        'AR': 3,
        'AZ': 4,
        'CA': 5,
        'CO': 6,
        'CT': 7,
        'FL': 8,
        'GA': 9,
        'HI': 10,
        'IA': 11,
        'ID': 12,
        'IL': 13,
        'IN': 14,
        'KS': 15,
        'KY': 16,
        'LA': 17,
        'MA': 18,
        'MD': 19,
        'ME': 20,
        'MI': 21,
        'MN': 22,
        'MO': 23,
        'MS': 24,
        'MT': 25,
        'NC': 26,
        'ND': 27,
        'NE': 28,
        'NH': 29,
        'NJ': 30,
        'NM': 31,
        'NV': 32,
        'NY': 33,
        'OH': 34,
        'OK': 35,
        'OR': 36,
        'PA': 37,
        'PR': 38,
        'RI': 39,
        'SC': 40,
        'SD': 41,
        'TN': 42,
        'TT': 43,
        'TX': 44,
        'UT': 45,
        'VA': 46,
        'VI': 47,
        'VT': 48,
        'WA': 49,
        'WI': 50,
        'WV': 51,
        'WY': 52
    }

    origin_state_name_dict = {
        'Alabama': 1,
        'Alaska': 2,
        'Arizona': 3,
        'Arkansas': 4,
        'California': 5,
        'Colorado': 6,
        'Connecticut': 7,
        'Florida': 8,
        'Georgia': 9,
        'Hawaii': 10,
        'Idaho': 11,
        'Illinois': 12,
        'Indiana': 13,
        'Iowa': 14,
        'Kansas': 15,
        'Kentucky': 16,
        'Louisiana': 17,
        'Maine': 18,
        'Maryland': 19,
        'Massachusetts': 20,
        'Michigan': 21,
        'Minnesota': 22,
        'Mississippi': 23,
        'Missouri': 24,
        'Montana': 25,
        'Nebraska': 26,
        'Nevada': 27,
        'New Hampshire': 28,
        'New Jersey': 29,
        'New Mexico': 30,
        'New York': 31,
        'North Carolina': 32,
        'North Dakota': 33,
        'Ohio': 34,
        'Oklahoma': 35,
        'Oregon': 36,
        'Pennsylvania': 37,
        'Puerto Rico': 38,
        'Rhode Island': 39,
        'South Carolina': 40,
        'South Dakota': 41,
        'Tennessee': 42,
        'Texas': 43,
        'U.S. Pacific Trust Territories and Possessions': 44,
        'U.S. Virgin Islands': 45,
        'Utah': 46,
        'Vermont': 47,
        'Virginia': 48,
        'Washington': 49,
        'West Virginia': 50,
        'Wisconsin': 51,
        'Wyoming': 52
    }

    selected_numeric_columns = raw_data.iloc[:, flight_num_indices]
    selected_categorical_columns = raw_data.iloc[:, flight_cat_indices]

    combined_selection = pd.concat([selected_numeric_columns, selected_categorical_columns], axis=1)

    combined_selection['FlightDate'] = combined_selection['FlightDate'].map(flight_date_dict)
    combined_selection['UniqueCarrier'] = combined_selection['UniqueCarrier'].map(carrier_dict)
    combined_selection['Carrier'] = combined_selection['Carrier'].map(carrier_dict)
    combined_selection['OriginState'] = combined_selection['OriginState'].map(origin_state_dict)
    combined_selection['OriginStateName'] = combined_selection['OriginStateName'].map(origin_state_name_dict)

    return combined_selection
def preprocess_nypd(raw_data):
    nypd_num_indices = [0, 3, 4, 18, 21, 30, 31, 32, 33]
    nypd_cat_indices = [1, 2, 7, 10, 11, 12, 13, 15, 17, 20, 27, 28, 29]

    boro_nm_dict = {
        'BRONX': 1, 'BROOKLYN': 2, 'MANHATTAN': 3, 'QUEENS': 4, 'STATEN ISLAND': 5
    }

    crm_atpt_cptd_cd_dict = {
        'ATTEMPTED': 0, 'COMPLETED': 1
    }

    juris_desc_dict = {
        'AMTRACK': 1,
        'DEPT OF CORRECTIONS': 2,
        'HEALTH & HOSP CORP': 3,
        'LONG ISLAND RAILRD': 4,
        'METRO NORTH': 5,
        'N.Y. HOUSING POLICE': 6,
        'N.Y. POLICE DEPT': 7,
        'N.Y. STATE PARKS': 8,
        'N.Y. STATE POLICE': 9,
        'N.Y. TRANSIT POLICE': 10,
        'NEW YORK CITY SHERIFF OFFICE': 11,
        'NYS DEPT TAX AND FINANCE': 12,
        'NYC PARKS': 13,
        'OTHER': 14,
        'PORT AUTHORITY': 15,
        'STATN IS RAPID TRANS': 16,
        'TRI-BORO BRDG TUNNL': 17,
        'U.S. PARK POLICE': 18
    }

    law_cat_cd_dict = {
        'FELONY': 1, 'MISDEMEANOR': 2, 'VIOLATION': 3
    }

    ofns_desc_dict = {
        'ABORTION': 1,
        'ADMINISTRATIVE CODE': 2,
        'AGRICULTURE & MRKTS LAW-UNCLASSIFIED': 3,
        'ALCOHOLIC BEVERAGE CONTROL LAW': 4,
        'ANTICIPATORY OFFENSES': 5,
        'ARSON': 6,
        'ASSAULT 3 & RELATED OFFENSES': 7,
        "BURGLAR'S TOOLS": 8,
        'BURGLARY': 9,
        'CHILD ABANDONMENT/NON SUPPORT': 10,
        'CRIMINAL MISCHIEF & RELATED OF': 11,
        'CRIMINAL TRESPASS': 12,
        'DANGEROUS DRUGS': 13,
        'DANGEROUS WEAPONS': 14,
        'DISORDERLY CONDUCT': 15,
        'ENDAN WELFARE INCOMP': 16,
        'ESCAPE 3': 17,
        'FELONY ASSAULT': 18,
        'FORGERY': 19,
        'FRAUDS': 20,
        'FRAUDULENT ACCOSTING': 21,
        'GAMBLING': 22,
        'GRAND LARCENY': 23,
        'GRAND LARCENY OF MOTOR VEHICLE': 24,
        'HARRASSMENT 2': 25,
        'HOMICIDE-NEGLIGENT,UNCLASSIFIE': 26,
        'INTOXICATED & IMPAIRED DRIVING': 27,
        'INTOXICATED/IMPAIRED DRIVING': 28,
        'JOSTLING': 29,
        'KIDNAPPING': 30,
        'KIDNAPPING & RELATED OFFENSES': 31,
        'LOITERING/GAMBLING (CARDS, DIC': 32,
        'MISCELLANEOUS PENAL LAW': 33,
        'MURDER & NON-NEGL. MANSLAUGHTER': 34,
        'NEW YORK CITY HEALTH CODE': 35,
        'NYS LAWS-UNCLASSIFIED FELONY': 36,
        'NYS LAWS-UNCLASSIFIED VIOLATION': 37,
        'OFF. AGNST PUB ORD SENSBLTY &': 38,
        'OFFENSES AGAINST PUBLIC ADMINI': 39,
        'OFFENSES AGAINST PUBLIC SAFETY': 40,
        'OFFENSES AGAINST THE PERSON': 41,
        'OFFENSES INVOLVING FRAUD': 42,
        'OFFENSES RELATED TO CHILDREN': 43,
        'OTHER OFFENSES RELATED TO THEF': 44,
        'OTHER STATE LAWS': 45,
        'OTHER STATE LAWS (NON PENAL LA': 46,
        'PETIT LARCENY': 47,
        'PETIT LARCENY OF MOTOR VEHICLE': 48,
        'POSSESSION OF STOLEN PROPERTY': 49,
        'PROSTITUTION & RELATED OFFENSES': 50,
        'RAPE': 51,
        'ROBBERY': 52,
        'SEX CRIMES': 53,
        'THEFT OF SERVICES': 54,
        'THEFT-FRAUD': 55,
        'UNAUTHORIZED USE OF A VEHICLE': 56,
        'VEHICLE AND TRAFFIC LAWS': 57
    }

    patrol_boro_dict = {
        'PATROL BORO BKLYN NORTH': 1,
        'PATROL BORO BKLYN SOUTH': 2,
        'PATROL BORO BRONX': 3,
        'PATROL BORO MAN NORTH': 4,
        'PATROL BORO MAN SOUTH': 5,
        'PATROL BORO QUEENS NORTH': 6,
        'PATROL BORO QUEENS SOUTH': 7,
        'PATROL BORO STATEN ISLAND': 8
    }

    prem_typ_desc_dict = {
        'ABANDONED BUILDING': 1,
        'AIRPORT TERMINAL': 2,
        'ATM': 3,
        'BANK': 4,
        'BAR/NIGHT CLUB': 5,
        'BEAUTY & NAIL SALON': 6,
        'BOOK/CARD': 7,
        'BRIDGE': 8,
        'BUS (NYC TRANSIT)': 9,
        'BUS (OTHER)': 10,
        'BUS STOP': 11,
        'BUS TERMINAL': 12,
        'CANDY STORE': 13,
        'CEMETERY': 14,
        'CHAIN STORE': 15,
        'CHECK CASHING BUSINESS': 16,
        'CHURCH': 17,
        'CLOTHING/BOUTIQUE': 18,
        'COMMERCIAL BUILDING': 19,
        'CONSTRUCTION SITE': 20,
        'DEPARTMENT STORE': 21,
        'DOCTOR/DENTIST OFFICE': 22,
        'DRUG STORE': 23,
        'DRY CLEANER/LAUNDRY': 24,
        'FACTORY/WAREHOUSE': 25,
        'FAST FOOD': 26,
        'FERRY/FERRY TERMINAL': 27,
        'FOOD SUPERMARKET': 28,
        'GAS STATION': 29,
        'GROCERY/BODEGA': 30,
        'GYM/FITNESS FACILITY': 31,
        'HIGHWAY/PARKWAY': 32,
        'HOSPITAL': 33,
        'HOTEL/MOTEL': 34,
        'JEWELRY': 35,
        'LIQUOR STORE': 36,
        'LOAN COMPANY': 37,
        'MAILBOX INSIDE': 38,
        'MAILBOX OUTSIDE': 39,
        'MARINA/PIER': 40,
        'MOSQUE': 41,
        'OPEN AREAS (OPEN LOTS)': 42,
        'OTHER': 43,
        'OTHER HOUSE OF WORSHIP': 44,
        'PARK/PLAYGROUND': 45,
        'PARKING LOT/GARAGE (PRIVATE)': 46,
        'PARKING LOT/GARAGE (PUBLIC)': 47,
        'PHOTO/COPY': 48,
        'PRIVATE/PAROCHIAL SCHOOL': 49,
        'PUBLIC BUILDING': 50,
        'PUBLIC SCHOOL': 51,
        'RESIDENCE - APT. HOUSE': 52,
        'RESIDENCE - PUBLIC HOUSING': 53,
        'RESIDENCE-HOUSE': 54,
        'RESTAURANT/DINER': 55,
        'SHOE': 56,
        'SMALL MERCHANT': 57,
        'SOCIAL CLUB/POLICY': 58,
        'STORAGE FACILITY': 59,
        'STORE UNCLASSIFIED': 60,
        'STREET': 61,
        'SYNAGOGUE': 62,
        'TAXI (LIVERY LICENSED)': 63,
        'TAXI (YELLOW LICENSED)': 64,
        'TAXI/LIVERY (UNLICENSED)': 65,
        'TELECOMM. STORE': 66,
        'TRANSIT - NYC SUBWAY': 67,
        'TRANSIT FACILITY (OTHER)': 68,
        'TRAMWAY': 69,
        'TUNNEL': 70,
        'VARIETY STORE': 71,
        'VIDEO STORE': 72,
    }

    vic_age_group_dict = {
        '-5': 1,
        '-43': 2,
        '-51': 3,
        '-55': 4,
        '-61': 5,
        '-76': 6,
        '-940': 7,
        '-942': 8,
        '-955': 9,
        '-956': 10,
        '-958': 11,
        '-968': 12,
        '-972': 13,
        '-974': 14,
        '18-24': 15,
        '25-44': 16,
        '45-64': 17,
        '65+': 18,
        '922': 19,
        '951': 20,
        '954': 21,
        '970': 22,
        '972': 23,
        '<18': 24,
        'UNKNOWN': 25
    }

    vic_race_dict = {'AMER IND': 1, 'ASIAN/PAC.ISL': 2, 'BLACK': 3, 'BLACK HISPANIC': 4, 'UNKNOWN': 5, 'WHITE': 6, 'WHITE HISPANIC': 7}
    vic_sex_dict = {'D': 1, 'E': 2, 'F': 3, 'M': 4, 'U': 5}

    selected_numeric_columns = raw_data.iloc[:, nypd_num_indices]
    selected_categorical_columns = raw_data.iloc[:, nypd_cat_indices]

    combined_selection = pd.concat([selected_numeric_columns, selected_categorical_columns], axis=1)

    combined_selection['boro_nm'] = combined_selection['boro_nm'].map(boro_nm_dict)  # 2
    combined_selection['boro_nm'] = combined_selection['boro_nm'].fillna(combined_selection['boro_nm'].mode()[0])

    combined_selection['cmplnt_fr_dt'] = combined_selection['cmplnt_fr_dt'].str.replace('-', '').astype(int)  # 3
    combined_selection['cmplnt_fr_tm'] = combined_selection['cmplnt_fr_tm'].str.replace(':', '').astype(int)  # 4

    combined_selection['crm_atpt_cptd_cd'] = combined_selection['crm_atpt_cptd_cd'].map(crm_atpt_cptd_cd_dict)  # 7, ignore for kp
    combined_selection['jurisdiction_code'] = combined_selection['jurisdiction_code'].fillna(combined_selection['jurisdiction_code'].mode()[0])  # 10
    combined_selection['juris_desc'] = combined_selection['juris_desc'].map(juris_desc_dict)  # 11, ignore for kp
    combined_selection['law_cat_cd'] = combined_selection['law_cat_cd'].map(law_cat_cd_dict)  # 13, ignore for kp

    combined_selection['ofns_desc'] = combined_selection['ofns_desc'].map(ofns_desc_dict)  # 15
    combined_selection['ofns_desc'] = combined_selection['ofns_desc'].fillna(combined_selection['ofns_desc'].mode()[0])

    combined_selection['patrol_boro'] = combined_selection['patrol_boro'].map(patrol_boro_dict)  # 17
    combined_selection['patrol_boro'] = combined_selection['patrol_boro'].fillna(combined_selection['patrol_boro'].mode()[0])

    combined_selection['pd_cd'] = combined_selection['pd_cd'].fillna(combined_selection['pd_cd'].mean())  # 18

    combined_selection['prem_typ_desc'] = combined_selection['prem_typ_desc'].map(prem_typ_desc_dict)  # 20
    combined_selection['prem_typ_desc'] = combined_selection['prem_typ_desc'].fillna(combined_selection['prem_typ_desc'].mode()[0])

    combined_selection['rpt_dt'] = combined_selection['rpt_dt'].str.replace('-', '').astype(int)  # 21

    combined_selection['vic_age_group'] = combined_selection['vic_age_group'].map(vic_age_group_dict)  # 27, ignore for kp
    combined_selection['vic_race'] = combined_selection['vic_race'].map(vic_race_dict)  # 28, ignore for kp
    combined_selection['vic_sex'] = combined_selection['vic_sex'].map(vic_sex_dict)  # 29, ignore for kp

    combined_selection['x_coord_cd'] = combined_selection['x_coord_cd'].fillna(combined_selection['x_coord_cd'].mean())  # 30
    combined_selection['y_coord_cd'] = combined_selection['y_coord_cd'].fillna(combined_selection['y_coord_cd'].mean())  # 31
    combined_selection['latitude'] = combined_selection['latitude'].fillna(combined_selection['latitude'].mean())  # 32
    combined_selection['longitude'] = combined_selection['longitude'].fillna(combined_selection['longitude'].mean())  # 33

    return combined_selection

def preprocess_data_kp(raw_data, name, num_index, cat_index, delimiter='|'):
    
    num_data = []  # To collect numerical data
    cat_data = []  # To collect categorical data

    pre = raw_data
    encoding_dict = {
    'AUTOMOBILE': 1,
    'BUILDING  ': 2,
    'FURNITURE ': 3,
    'HOUSEHOLD ': 4,
    'MACHINERY ': 5
    }
    encoding_dict_o = {
    '1-URGENT': 1,
    '2-HIGH': 2,
    '3-MEDIUM': 3,
    '4-NOT SPECIFIED': 4,
    '5-LOW': 5
    }
    encoding_dict_2 = {
    'F': 1,
    'O': 2,
    'P': 3
    }
    if 'DS_001' in name or 'DS_002' in name or 'DS_003'in name:
        pre = pre.replace({6:encoding_dict})

    elif  'orders' in name:
        pre[6]=pre[6].str.replace('Clerk#', '')
        pre = pre.replace({5:encoding_dict_o})
        pre = pre.replace({2:encoding_dict_2})


    # Collecting numerical and categorical data separately
    if len(num_index)>0:
        cate_list = [pre[j] for j in num_index]
        num_list = [pre[j] for j in cat_index]
        num_data.append(num_list)
        cat_data.append(cate_list)
        num_data = num_data[0]
        cat_data = cat_data[0]
        # Convert lists to numpy arrays for processing
        num_features = np.array(num_data)
        cat_features = np.array(cat_data)
        num = []
        cat = []
        for i in range(len(num_features)):
            num.append(num_features[i].reshape(-1,1))
        for j in range(len(cat_features)):
            cat.append(cat_features[j].reshape(-1,1))
        num = tuple(num)
        cat = tuple(cat)
        num_features = np.hstack(num)
        cat_features = np.hstack(cat)
    else:
        num_list = [pre[j] for j in cat_index]
        num_data.append(num_list)
        num_data = num_data[0]
        num_features = np.array(num_data)
        num = []
        for i in range(len(num_features)):
            num.append(num_features[i].reshape(-1,1))
        num = tuple(num)
        num_features = np.hstack(num)
        cat_features = None
        return num_features
    '''
    num_1_features = num_features[0]
    num_2_features = num_features[1]
    cat_1_features = cat_features[0]
    cat_2_features = cat_features[1]
    num_1_features = num_1_features.reshape(-1,1)
    num_2_features = num_2_features.reshape(-1,1)
    cat_1_features = cat_1_features.reshape(-1,1)
    cat_2_features = cat_2_features.reshape(-1,1)
    num_features = np.hstack((num_1_features,num_2_features))
    cat_features = np.hstack((cat_1_features,cat_2_features))
    '''
    # Scale the numerical features

    # Combine scaled numerical and original categorical features
    return num_features, cat_features

def classify_data_scaler(df, model,scaler_path):
    scaler = joblib.load(scaler_path)
    df_scaled = scaler.transform(df)
    predictions = model.predict(df_scaled)
    return predictions 
def classify_data_scaler_kp(num,cat, model,scaler_path):
    scaler = joblib.load(scaler_path)
    df_scaled = scaler.transform(num)
    df = np.hstack((df_scaled,cat))
    predictions = model.predict(df)
    return predictions 
def classify_data_scaler_kp_none(num, model,scaler_path):
    scaler = joblib.load(scaler_path)
    df_scaled = scaler.transform(num)
    predictions = model.predict(df_scaled)
    return predictions 
def classify_data(df,model):
    predictions = model.predict(df)
    return predictions 
def add_labels_to_chunk(chunk, labels):
    chunk['label'] = labels
    return chunk
def classify_chunk_scaler_kp(chunk,name,model,scaler_path,num_i,cat_i):

    warnings.filterwarnings("ignore")

    if "flight" in name:
        pre_chunk = preprocess_flight(chunk)       
        num = pre_chunk.iloc[:,:4]
        cat = pre_chunk.iloc[:,4:]
        start_time = time.time()

        predictions = classify_data_scaler_kp(num,cat,model,scaler_path)
        end_time = time.time()
        class_time = end_time - start_time
        chunk_with_labels = add_labels_to_chunk(chunk,predictions)
    elif "nypd" in name:
        pre_chunk = preprocess_nypd(chunk)
        num = pre_chunk.iloc[:,:9]
        cat = pre_chunk.iloc[:,9:]
        start_time = time.time()
        predictions = classify_data_scaler_kp(num,cat,model,scaler_path)
        end_time = time.time()
        class_time = end_time - start_time
        chunk_with_labels = add_labels_to_chunk(chunk,predictions)
    elif "partsupp" in name:
        num= preprocess_data_kp(chunk,name,num_i,cat_i)
        start_time =time.time()
        predictions = classify_data_scaler_kp_none(num, model,scaler_path)
        end_time = time.time()
        class_time = end_time - start_time
        chunk_with_labels = add_labels_to_chunk(chunk, predictions)
        
    else:
        num,cat = preprocess_data_kp(chunk,name,num_i,cat_i)
        start_time =time.time()
        predictions = classify_data_scaler_kp(num,cat, model,scaler_path)
        end_time = time.time()
        class_time = end_time - start_time
        chunk_with_labels = add_labels_to_chunk(chunk, predictions)
    return chunk_with_labels,class_time
def classify_chunk_scaler_km(chunk,model,scaler_path):
    warnings.filterwarnings("ignore")
    start_time =time.time()
    processed_data = preprocess_data_km(chunk)

    predictions = classify_data_scaler(processed_data, model,scaler_path)
    print(predictions)
    end_time = time.time()
    class_time = end_time - start_time
    chunk_with_labels = add_labels_to_chunk(chunk, predictions)
    return chunk_with_labels,class_time
def classify_chunk_km(chunk,model):
    warnings.filterwarnings("ignore")
    start_time =time.time()
    processed_data = preprocess_data_km(chunk)
    predictions = classify_data(processed_data, model)
    print(predictions)
    end_time = time.time()
    class_time = end_time - start_time
    chunk_with_labels = add_labels_to_chunk(chunk, predictions)
    return chunk_with_labels,class_time
def classify_chunk_kp(chunk,name,model,num_i,cat_i):
    if 'flight' in name:
        pre_chunk = preprocess_flight(chunk)
        start_time = time.time()
        
        predictions = classify_data(pre_chunk,model)
        end_time = time.time()
        class_time = end_time - start_time
        chunk_with_labels = add_labels_to_chunk(chunk,predictions)
    elif 'nypd' in name:
        pre_chunk = preprocess_nypd(chunk)
        start_time = time.time()
        
        predictions = classify_data(pre_chunk,model)
        end_time = time.time()
        class_time = end_time - start_time
        chunk_with_labels = add_labels_to_chunk(chunk,predictions)

    else:     
        warnings.filterwarnings("ignore")
        if len(num_i)>0:
            start_time =time.time()
            num,cat = preprocess_data_kp(chunk,name,num_i,cat_i)
            processed_data = np.hstack((num,cat))
            predictions = classify_data(processed_data, model)
            end_time = time.time()
            class_time = end_time - start_time
            chunk_with_labels = add_labels_to_chunk(chunk, predictions)
        else:
            start_time =time.time()
            processed_data = preprocess_data_kp(chunk,name,num_i,cat_i)

            predictions = classify_data(processed_data, model)
            end_time = time.time()
            class_time = end_time - start_time
            chunk_with_labels = add_labels_to_chunk(chunk, predictions)
    return chunk_with_labels,class_time
def classify_chunk(chunk,chunk_og,model):
    warnings.filterwarnings("ignore")
    #processed_data = preprocess_data(chunk)
    start_time =time.time()
    predictions = classify_data(chunk, model)

    end_time = time.time()
    class_time = end_time - start_time
    chunk_with_labels = add_labels_to_chunk(chunk_og, predictions)
    return chunk_with_labels,class_time
def classify_chunk_base(chunk,label):
    labels = [label]* chunk.shape[0]
    chunk_with_labels = add_labels_to_chunk(chunk,labels)
    return chunk_with_labels
def classify_chunk_base_one(chunk):
    size = chunk.shape[0]
    predictions = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] * (size // 10) + [0, 1, 2, 3, 4, 5, 6, 7, 8, 9][:size % 10]
    chunk_with_labels = add_labels_to_chunk(chunk,predictions)
    return chunk_with_labels