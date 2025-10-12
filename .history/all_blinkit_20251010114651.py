import pandas as pd

url = 'https://drive.google.com/file/d/1C3WIWJYYsBA_FgB8faDjr2TrGQs-mEp-/view?usp=drive_link'
df = pd.read_csv(url)
print(df.head())
