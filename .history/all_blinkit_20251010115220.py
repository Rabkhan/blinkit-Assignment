import pandas as pd

url = 'https://drive.google.com/uc?export=download&id=1C3WIWJYYsBA_FgB8faDjr2TrGQs-mEp-'
df = pd.read_csv(url)
print(df.head())
