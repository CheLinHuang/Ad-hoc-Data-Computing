import pandas as pd
import feather
import glob

for csvFile in glob.glob('*.csv'):
    df = pd.read_csv(csvFile)
    name = csvFile.split('.')
    print("generate: ", name[0] + ".feather")
    feather.write_dataframe(df, name[0] + ".feather")
