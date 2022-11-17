import pandas as pd

##### 1 ######
df = pd.read_csv('people/people_1.txt',sep=r"\s+", header=None, names=["FirstName", "LastName", "Email", "Phone", "Address1","Address2","Address3"])
df = df[1:]
df['FirstName'] = df['FirstName'].str.capitalize()
df['LastName'] = df['LastName'].str.capitalize()
df['Phone'] = df.Phone.str.split("-").str.join('')
df['Phone'] = df['Phone'].astype(str).apply(lambda x: x[:3]+'-'+x[3:6]+'-'+x[6:10])
df['Address1'] = df['Address1'].map(lambda x: x.lstrip('#'))
df['Address1'] = df['Address1'].str.replace('No.', '')
df['Address'] = df['Address1'] + ' ' + df['Address2'] + ' '+ df['Address3']
df = df.drop(columns=['Address1', 'Address2','Address3'])
df = df.drop_duplicates()
print(df)

##### 2 ######
import json

f = open('movie.json')
data = json.load(f)
df2 = pd.DataFrame.from_dict(pd.json_normalize(data["movie"]))

totalRow = len(df2.index)
eachFile = totalRow // 8
splitSoFar = 0
for i in range(1, 8):
    tempDf = df2.iloc[splitSoFar:splitSoFar+eachFile]
    splitSoFar += eachFile
    # export to json
    result = tempDf.to_json(orient="records")
    parsed = json.loads(result)
    json_object = json.dumps(parsed, indent=4)
    # Writing to sample.json
    with open("df"+str(i)+".json", "w") as outfile:
        outfile.write(json_object)
    #print(len(tempDf.index))

df8 = df2.iloc[splitSoFar:]
result = df8.to_json(orient="records")
parsed = json.loads(result)
json_object = json.dumps(parsed, indent=4)
with open("df8.json", "w") as outfile:
    outfile.write(json_object)

#print(len(df8.index))