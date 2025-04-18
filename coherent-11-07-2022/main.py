import pandas as pd
import sqlite3 as sq

conn = sq.connect('synthea.db')
csv_files = ["patients", "conditions", "procedures", "imaging_studies", "encounters"]

for file in csv_files:
    df = pd.read_csv(f"csv/{file}.csv")
    df.to_sql(file, conn, if_exists='replace', index=False)
print("The data has loaded")

# find patients diagnosed with CAD 
print("\nFinding patients diagnosed with CAD")

# cad_patients_query = "SELECT DISTINCT c.Patient FROM conditions c WHERE LOWER(c.DESCRIPTION) LIKE '%coronary artery disease%'"
'''results from this query didn't match anything I got 0 as my output...'''
cad_patients_query = """
SELECT DISTINCT c.PATIENT, c.DESCRIPTION
FROM conditions c
WHERE LOWER(c.DESCRIPTION) LIKE '%coronary%'
   OR LOWER(c.DESCRIPTION) LIKE '%artery%'
   OR LOWER(c.DESCRIPTION) LIKE '%cad%'
   OR LOWER(c.DESCRIPTION) LIKE '%heart disease%'
   OR LOWER(c.DESCRIPTION) LIKE '%atherosclerosis%'
"""
cad_patients_df = pd.read_sql_query(cad_patients_query, conn)
print("CAD patients found: ", len(cad_patients_df)) # found 565

# join the patients table to get gender and the race 
print("\\nJoining CAD patients with demographics...")
cad_demo_query = """
    SELECT p.Id AS patient_id, p.GENDER, p.RACE
FROM patients p
JOIN (
    SELECT DISTINCT PATIENT
    FROM conditions
    WHERE LOWER(DESCRIPTION) LIKE '%coronary%'
       OR LOWER(DESCRIPTION) LIKE '%artery%'
       OR LOWER(DESCRIPTION) LIKE '%cad%'
       OR LOWER(DESCRIPTION) LIKE '%heart disease%'
       OR LOWER(DESCRIPTION) LIKE '%atherosclerosis%'
) cad
ON p.Id = cad.PATIENT 
"""
cad_demo_df = pd.read_sql_query(cad_demo_query, conn)
print(cad_demo_df.head()) # 5 rows back 
'''
Only 5 patients in the entire dataset had a CAD-related diagnosis using LIKE filters.
This can be becuase of the small dataset that, it randomly generates populations, and only a few might have CAD.
'''

# find procedure orders related to target tests 
keywords = ['computed tomography', 'ultrasound', 'calcium', 'ct angiography', 'thoracic']
for keyword in keywords:
    query = f"""
    SELECT *
    FROM procedures
    WHERE LOWER(DESCRIPTION) LIKE '%{keyword}%'
    """
    df = pd.read_sql_query(query, conn)
    print(f"'{keyword}' matches: {len(df)}")


print("\nAnalyzing echocardiograms from imaging_studies...")

# Step 1: Get all echocardiograms (ultrasound of the heart)
echos_query = """
SELECT i.PATIENT
FROM imaging_studies i
WHERE LOWER(i.MODALITY_DESCRIPTION) LIKE '%ultrasound%'
  AND LOWER(i.BODYSITE_DESCRIPTION) LIKE '%heart%'
"""
echos_df = pd.read_sql_query(echos_query, conn)

# Step 2: Get CAD patients (already loaded as cad_patients_df)
# Step 3: Inner join both to get CAD patients who had echocardiograms
cad_with_echos = pd.merge(cad_patients_df, echos_df, on='PATIENT', how='inner')

# Step 4: Join with patients table to get gender and race
cad_demo_query = """
SELECT p.Id AS patient_id, p.GENDER, p.RACE
FROM patients p
"""
cad_demo_df = pd.read_sql_query(cad_demo_query, conn)

# Step 5: Merge all together
final_df = pd.merge(cad_with_echos, cad_demo_df, left_on='PATIENT', right_on='patient_id', how='left')

# Step 6: Group by gender and race
grouped_summary = final_df.groupby(['GENDER', 'RACE']).size().reset_index(name='Echocardiogram_Count')

print("\nEchocardiogram frequency by gender and race:")
print(grouped_summary)


'''==========================MOST PREVALENT DATA WITHIN DATASETS============================'''


df_conditions = pd.read_sql_query("SELECT DESCRIPTION FROM conditions", conn)
top_conditions = df_conditions['DESCRIPTION'].value_counts().head(10)
print("Top Diagnoses:\n", top_conditions)


df_procedures = pd.read_sql_query("SELECT DESCRIPTION FROM procedures", conn)
top_procedures = df_procedures['DESCRIPTION'].value_counts().head(10)
print("\nTop Procedures:\n", top_procedures)


df_imaging = pd.read_sql_query("SELECT MODALITY_DESCRIPTION, BODYSITE_DESCRIPTION FROM imaging_studies", conn)
top_modalities = df_imaging['MODALITY_DESCRIPTION'].value_counts().head(5)
top_body_parts = df_imaging['BODYSITE_DESCRIPTION'].value_counts().head(5)

print("\nTop Imaging Modalities:\n", top_modalities)
print("\nTop Imaged Body Parts:\n", top_body_parts)



'''=============================================='''

''' try and find the most prevalent data within the datasets that I already have in use '''


sql1_query = """
     SELECT p.GENDER, p.RACE, c.DESCRIPTION
        FROM conditions c
        JOIN patients p ON c.PATIENT = p.Id
        WHERE LOWER(c.DESCRIPTION) LIKE '%hypertension%'
        OR LOWER(c.DESCRIPTION) LIKE '%diabetes%'
    """
sql2_query = """
    SELECT p.GENDER, p.RACE, i.MODALITY_DESCRIPTION
    FROM imaging_studies i
    JOIN patients p ON i.PATIENT = p.Id
    WHERE LOWER(i.MODALITY_DESCRIPTION) LIKE '%ultrasound%'
    """

sql3_query = """
     SELECT p.BIRTHDATE, c.START, c.DESCRIPTION
        FROM conditions c
        JOIN patients p ON c.PATIENT = p.Id
        WHERE LOWER(c.DESCRIPTION) LIKE '%coronary%'
           OR LOWER(c.DESCRIPTION) LIKE '%artery%'
           OR LOWER(c.DESCRIPTION) LIKE '%heart disease%'
           OR LOWER(c.DESCRIPTION) LIKE '%cad%'
           OR LOWER(c.DESCRIPTION) LIKE '%atherosclerosis%'
        """

sql4_query = """
    SELECT p.GENDER, pr.DESCRIPTION
        FROM procedures pr
        JOIN patients p ON pr.PATIENT = p.Id
        WHERE LOWER(pr.DESCRIPTION) LIKE '%ct%'
    """

queries = {
     "Hypertension & Diabetes by Gender and Race": sql1_query,
    "Ultrasound Usage by Gender and Race": sql2_query,
    "Coronary Diagnoses by Birth Decade": sql3_query,
    "CT Usage by Gender": sql4_query
}

results = {}
for title, query in queries.items():
    df = pd.read_sql_query(query, conn)
    results[title] = df
    print(f"\n=========={title}========")
    print(df.count())


