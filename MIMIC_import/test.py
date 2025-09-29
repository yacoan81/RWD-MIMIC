import json
import pandas as pd

def flatten_dict(d, parent_key="", sep="_"):
    """
    Recursively flatten a dictionary or list into flat key-value pairs
    with numbered suffixes for lists.
    """
    items = {}
    if isinstance(d, dict):
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, (dict, list)):
                items.update(flatten_dict(v, new_key, sep=sep))
            else:
                items[new_key] = v
    elif isinstance(d, list):
        for i, v in enumerate(d, start=1):
            new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
            if isinstance(v, (dict, list)):
                items.update(flatten_dict(v, new_key, sep=sep))
            else:
                items[new_key] = v
    return items

# Example row (strings from your CSV)
row = {
    "id": "0a8eebfd-a352-522e-89f0-1d4a13abdebc",
    "name": [{'use': 'official', 'family': 'Patient_10000032'}],
    "gender": "female",
    "birthDate": "2128-05-06",
    "extension": [
        {
            'url': 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race',
            'extension': [
                {'url': 'ombCategory', 'valueCoding': {'code': '2106-3','system':'urn:oid:2.16.840.1.113883.6.238','display':'White'}},
                {'url':'text','valueString':'White'}
            ]
        },
        {
            'url':'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity',
            'extension': [
                {'url':'ombCategory','valueCoding':{'code':'2186-5','system':'urn:oid:2.16.840.1.113883.6.238','display':'Not Hispanic or Latino'}},
                {'url':'text','valueString':'Not Hispanic or Latino'}
            ]
        },
        {'url':'http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex','valueCode':'F'}
    ],
    "identifier":[{'value':'10000032','system':'http://mimic.mit.edu/fhir/mimic/identifier/patient'}],
    "resourceType":"Patient",
    "communication":[{'language':{'coding':[{'code':'en','system':'urn:ietf:bcp:47'}]}}],
    "deceasedDateTime":"2180-09-09",
    "meta_profile":['http://mimic.mit.edu/fhir/mimic/StructureDefinition/mimic-patient'],
    "maritalStatus_coding":[{'code':'W','system':'http://terminology.hl7.org/CodeSystem/v3-MaritalStatus'}],
    "managingOrganization_reference":"Organization/ee172322-118b-5716-abbc-18e4c5437e15"
}

flat_row = flatten_dict(row)
df = pd.DataFrame([flat_row])
print(df.head(1).T)  # print transposed to see columns clearly
