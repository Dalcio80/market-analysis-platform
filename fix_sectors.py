import pandas as pd

SIC_EXTRA = {
    '7363': 'XLI', '1600': 'XLI', '6792': 'XLF',
    '6799': 'XLF', '2211': 'XLP', '7340': 'XLI',
    '5065': 'XLK', '1500': 'XLI', '1520': 'XLI',
    '1731': 'XLI',
}

df = pd.read_csv('data/insider_latest.csv')
df['sic'] = df['sic'].astype(str).str.replace('.0', '', regex=False)
mask = df['sector'] == 'Other'
df.loc[mask, 'sector'] = df.loc[mask, 'sic'].map(SIC_EXTRA).fillna('Other')
df.to_csv('data/insider_latest.csv', index=False)
print('Aggiornato. Nuova aggregazione:')
print(df[df['transaction_code']=='P'].groupby('sector').size().sort_values(ascending=False))
