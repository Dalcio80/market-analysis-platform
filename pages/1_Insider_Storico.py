# pages/1_Insider_Storico.py
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import yfinance as yf
from datetime import datetime, timedelta
import os

st.set_page_config(page_title='Analisi Storica Insider', layout='wide')

st.title('📈 Analisi Storica Insider vs ETF')
st.caption('Confronto tra attività insider e andamento del settore nel tempo')

DATA_PATH = 'data/insider_latest.csv'

SECTOR_ETF = {
    'XLK': 'XLK — Technology',
    'XLF': 'XLF — Financials',
    'XLE': 'XLE — Energy',
    'XLV': 'XLV — Health Care',
    'XLI': 'XLI — Industrials',
    'XLC': 'XLC — Communication',
    'XLY': 'XLY — Consumer Discret.',
    'XLP': 'XLP — Consumer Staples',
    'XLB': 'XLB — Materials',
    'XLRE': 'XLRE — Real Estate',
    'XLU': 'XLU — Utilities',
}

if not os.path.exists(DATA_PATH):
    st.error('Nessun dato disponibile.')
    st.stop()

@st.cache_data(ttl=3600)
def load_insider_data():
    df = pd.read_csv(DATA_PATH)
    df['filing_date'] = pd.to_datetime(df['filing_date'], errors='coerce')
    df = df.dropna(subset=['filing_date'])
    df['total_value'] = pd.to_numeric(df['total_value'], errors='coerce').fillna(0)
    df['shares'] = pd.to_numeric(df['shares'], errors='coerce').fillna(0)
    return df[df['transaction_code'] == 'P'].copy()

@st.cache_data(ttl=3600)
def load_etf_price(ticker, period_days):
    try:
        end = datetime.now()
        start = end - timedelta(days=period_days + 30)
        df = yf.download(ticker, start=start, end=end, progress=False)
        if df.empty:
            return pd.DataFrame()
        df = df[['Close']].reset_index()
        df.columns = ['date', 'close']
        df['date'] = pd.to_datetime(df['date'])
        return df
    except:
        return pd.DataFrame()

df_insider = load_insider_data()

# --- SIDEBAR ---
st.sidebar.header('Impostazioni grafico')

etf_sel = st.sidebar.selectbox(
    'Settore ETF',
    options=list(SECTOR_ETF.keys()),
    format_func=lambda x: SECTOR_ETF[x],
    index=0
)

metrica = st.sidebar.selectbox(
    'Metrica insider',
    options=[
        'Ultimi 25 acquisti (giorni tra acquisti)',
        'Acquisti rolling 30 giorni',
        'Valore ($) rolling 30 giorni',
    ],
    index=0
)

periodo_storico = st.sidebar.selectbox(
    'Periodo storico',
    options=[90, 180, 365],
    format_func=lambda x: f'Ultimi {x} giorni' if x < 365 else '1 anno',
    index=0
)

# --- DATI ---
df_sector = df_insider[df_insider['sector'] == etf_sel].copy()
df_sector = df_sector.sort_values('filing_date')

if df_sector.empty:
    st.warning(f'Nessun acquisto insider trovato per {etf_sel}.')
    st.stop()

# Calcola metrica rolling
if metrica == 'Ultimi 25 acquisti (giorni tra acquisti)':
    dates = df_sector['filing_date'].sort_values().reset_index(drop=True)
    rolling_data = []
    for i in range(len(dates)):
        if i >= 24:
            span = (dates[i] - dates[i - 24]).days
            rolling_data.append({'date': dates[i], 'metric': span})
        else:
            rolling_data.append({'date': dates[i], 'metric': None})
    df_rolling = pd.DataFrame(rolling_data).dropna()
    metrica_label = 'Giorni tra ultimi 25 acquisti'
    if not df_rolling.empty:
        max_val = df_rolling['metric'].max()
        min_val = df_rolling['metric'].min()
        df_rolling['metric'] = max_val - df_rolling['metric'] + min_val

elif metrica == 'Acquisti rolling 30 giorni':
    df_daily = df_sector.groupby(df_sector['filing_date'].dt.date).size().reset_index(name='count')
    df_daily.columns = ['date', 'count']
    df_daily['date'] = pd.to_datetime(df_daily['date'])
    df_daily = df_daily.set_index('date').resample('D').sum().fillna(0).reset_index()
    df_daily['metric'] = df_daily['count'].rolling(30, min_periods=1).sum()
    df_rolling = df_daily[['date', 'metric']]
    metrica_label = 'N° acquisti (rolling 30 giorni)'

else:
    df_daily = df_sector.groupby(df_sector['filing_date'].dt.date)['total_value'].sum().reset_index()
    df_daily.columns = ['date', 'total_value']
    df_daily['date'] = pd.to_datetime(df_daily['date'])
    df_daily = df_daily.set_index('date').resample('D').sum().fillna(0).reset_index()
    df_daily['metric'] = df_daily['total_value'].rolling(30, min_periods=1).sum() / 1e6
    df_rolling = df_daily[['date', 'metric']]
    metrica_label = 'Valore acquisti rolling 30 giorni ($M)'

# Filtra per periodo
date_from = datetime.now() - timedelta(days=periodo_storico)
df_rolling = df_rolling[pd.to_datetime(df_rolling['date']) >= date_from]

# Scarica prezzo ETF
with st.spinner(f'Scaricando prezzo {etf_sel}...'):
    df_etf = load_etf_price(etf_sel, periodo_storico)

if df_rolling.empty:
    st.warning('Dati insufficienti per il grafico nel periodo selezionato.')
    st.info('Suggerimento: prova con "Acquisti rolling 30 giorni" o seleziona un settore con più dati (XLF, XLI, XLV).')
    st.stop()

# Soglie storiche
mean_val = df_rolling['metric'].mean()
low_val = df_rolling['metric'].quantile(0.25)

# --- GRAFICO ---
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df_rolling['date'],
    y=df_rolling['metric'],
    name=metrica_label,
    line=dict(color='#378ADD', width=2),
    yaxis='y1',
    fill='tozeroy',
    fillcolor='rgba(55,138,221,0.1)',
))

fig.add_hline(
    y=mean_val,
    line_dash='dash',
    line_color='#1D9E75',
    line_width=1.5,
    annotation_text=f'Media: {mean_val:.1f}',
    annotation_position='bottom left',
    yref='y1'
)

fig.add_hline(
    y=low_val,
    line_dash='dash',
    line_color='#D85A30',
    line_width=1.5,
    annotation_text=f'25°p: {low_val:.1f}',
    annotation_position='bottom left',
    yref='y1'
)

if not df_etf.empty:
    df_etf_filtered = df_etf[df_etf['date'] >= date_from]
    fig.add_trace(go.Scatter(
        x=df_etf_filtered['date'],
        y=df_etf_filtered['close'],
        name=f'{etf_sel} (prezzo)',
        line=dict(color='#F1EFE8', width=2),
        yaxis='y2',
    ))

fig.update_layout(
    title=f'Corporate Insider Buys — {etf_sel}',
    plot_bgcolor='#1a1a2e',
    paper_bgcolor='rgba(0,0,0,0)',
    height=500,
    yaxis=dict(
        title=metrica_label,
        side='left',
        showgrid=True,
        gridcolor='rgba(255,255,255,0.08)',
        tickfont=dict(color='#B4B2A9'),
        color='#B4B2A9',
    ),
    yaxis2=dict(
        title=f'{etf_sel} prezzo ($)',
        side='right',
        overlaying='y',
        showgrid=False,
        tickfont=dict(color='#888780'),
        color='#888780',
    ),
    legend=dict(
        orientation='h',
        y=1.05,
        font=dict(color='#B4B2A9')
    ),
    xaxis=dict(
        showgrid=True,
        gridcolor='rgba(255,255,255,0.08)',
        tickfont=dict(color='#B4B2A9'),
    ),
    font=dict(color='#B4B2A9'),
)

st.plotly_chart(fig, use_container_width=True)

# --- METRICHE ---
col1, col2, col3, col4 = st.columns(4)
col1.metric('Acquisti totali periodo', len(df_sector))
col2.metric('Valore totale', f"${df_sector['total_value'].sum()/1e6:.1f}M")
col3.metric('Acquisto più grande', f"${df_sector['total_value'].max():,.0f}")
col4.metric('Aziende uniche', df_sector['company'].nunique())

# --- TABELLA ---
st.subheader(f'Tutti gli acquisti — {etf_sel}')
df_show = df_sector[[
    'filing_date', 'ticker', 'company', 'insider_name',
    'insider_role', 'shares', 'price_per_share', 'total_value'
]].copy()
df_show['filing_date'] = df_show['filing_date'].dt.strftime('%Y-%m-%d')
df_show['total_value'] = df_show['total_value'].apply(lambda x: f'${x:,.0f}')
df_show['shares'] = df_show['shares'].apply(lambda x: f'{x:,.0f}')
df_show['price_per_share'] = df_show['price_per_share'].apply(lambda x: f'${x:.2f}')
df_show.columns = ['Data', 'Ticker', 'Società', 'Insider', 'Ruolo', 'Azioni', 'Prezzo', 'Valore Tot.']
st.dataframe(
    df_show.sort_values('Data', ascending=False),
    use_container_width=True,
    hide_index=True,
    height=400
)