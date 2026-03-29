import pandas as pd

df = pd.read_csv('data/insider_latest.csv')
other = df[(df['sector'] == 'Other') & (df['transaction_code'] == 'P')]
print(f'Totale Other con acquisti P: {len(other)}')
print(other[['company', 'ticker', 'sic']].drop_duplicates().head(20).to_string())
