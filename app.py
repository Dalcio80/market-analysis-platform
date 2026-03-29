# app.py - Dashboard principale Market Analysis Platform
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta

st.set_page_config(
    page_title='Market Analysis Platform',
    page_icon='📊',
    layout='wide'
)

st.title('📊 Market Analysis Platform')
st.caption(f'Ultimo aggiornamento dati: {datetime.now().strftime("%d/%m/%Y %H:%M")}')

DATA_PATH = 'data/insider_latest.csv'

SECTOR_LABELS = {
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
    'Other': 'Other — Non classificato',
}

if not os.path.exists(DATA_PATH):
    st.error('Nessun dato disponibile. Esegui prima: py -3.12 modules/insider_buying.py')
    st.stop()

@st.cache_data(ttl=3600)
def load_data():
    df = pd.read_csv(DATA_PATH)
    df['filing_date'] = pd.to_datetime(df['filing_date'], errors='coerce')
    df = df.dropna(subset=['filing_date'])
    df['total_value'] = pd.to_numeric(df['total_value'], errors='coerce').fillna(0)
    df['shares'] = pd.to_numeric(df['shares'], errors='coerce').fillna(0)
    df['price_per_share'] = pd.to_numeric(df['price_per_share'], errors='coerce').fillna(0)
    return df.sort_values('filing_date', ascending=False)

df_full = load_data()

# Solo acquisti open market
df_buys_full = df_full[df_full['transaction_code'] == 'P'].copy()

# --- SIDEBAR ---
st.sidebar.header('Filtri')

st.sidebar.subheader('Periodo')
periodo_preset = st.sidebar.selectbox(
    'Preimpostato',
    options=['Ultima settimana', 'Ultimi 30 giorni', 'Ultimi 60 giorni',
             'Ultimi 90 giorni', 'Ultimi 180 giorni', 'Personalizzato'],
    index=1
)

preset_map = {
    'Ultima settimana': 7,
    'Ultimi 30 giorni': 30,
    'Ultimi 60 giorni': 60,
    'Ultimi 90 giorni': 90,
    'Ultimi 180 giorni': 180,
}

if periodo_preset == 'Personalizzato':
    periodo = st.sidebar.number_input(
        'Numero di giorni (da oggi)',
        min_value=1,
        max_value=365,
        value=30,
        step=1
    )
else:
    periodo = preset_map[periodo_preset]

date_from = datetime.now() - timedelta(days=periodo)
df_buys = df_buys_full[df_buys_full['filing_date'] >= date_from].copy()

tutti_settori = ['Tutti'] + sorted(
    [s for s in df_buys['sector'].unique() if s != 'Other']
) + ['Other']

settore_sel = st.sidebar.selectbox(
    'Settore ETF',
    options=tutti_settori,
    format_func=lambda x: SECTOR_LABELS.get(x, x),
    index=0
)

# Applica filtro settore
df_filtered = df_buys if settore_sel == 'Tutti' else df_buys[df_buys['sector'] == settore_sel]

# --- METRICHE ---
st.header('Insider Buying — SEC EDGAR Form 4 (solo acquisti open market)')

col1, col2, col3, col4 = st.columns(4)
col1.metric('Acquisti nel periodo', len(df_filtered))
col2.metric('Aziende uniche', df_filtered['company'].nunique())
col3.metric(
    'Valore totale',
    f"${df_filtered['total_value'].sum()/1e6:.1f}M"
)
col4.metric(
    'Azioni totali',
    f"{df_filtered['shares'].sum():,.0f}"
)

st.divider()

# --- GRAFICI ---
if settore_sel == 'Tutti':
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader('Valore acquisti per settore ($)')
        sector_val = df_filtered.groupby('sector')['total_value'].sum().reset_index()
        sector_val = sector_val.sort_values('total_value', ascending=False)
        sector_val['valore_M'] = sector_val['total_value'] / 1e6
        sector_val['label'] = sector_val['sector'].map(SECTOR_LABELS).fillna(sector_val['sector'])

        fig_val = px.bar(
            sector_val,
            x='sector',
            y='valore_M',
            color='valore_M',
            color_continuous_scale='Blues',
            labels={'valore_M': 'Valore ($M)', 'sector': 'Settore'},
            text=sector_val['valore_M'].apply(lambda x: f'${x:.1f}M')
        )
        fig_val.update_traces(textposition='outside')
        fig_val.update_layout(
            coloraxis_showscale=False,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=400
        )
        st.plotly_chart(fig_val, use_container_width=True)

    with col_right:
        st.subheader('Numero acquisti per settore')
        sector_count = df_filtered.groupby('sector').size().reset_index(name='count')
        sector_count = sector_count[sector_count['sector'] != 'Other']
        sector_count = sector_count.sort_values('count', ascending=False)

        fig_pie = px.pie(
            sector_count,
            values='count',
            names='sector',
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig_pie.update_layout(height=400)
        st.plotly_chart(fig_pie, use_container_width=True)

    # Andamento valore nel tempo
    st.subheader('Andamento valore acquisti nel tempo')
    df_time = df_filtered[df_filtered['sector'] != 'Other'].copy()
    df_time['week'] = df_time['filing_date'].dt.to_period('W').apply(lambda r: r.start_time)
    df_weekly = df_time.groupby(['week', 'sector'])['total_value'].sum().reset_index()
    df_weekly['valore_M'] = df_weekly['total_value'] / 1e6

    fig_line = px.line(
        df_weekly,
        x='week',
        y='valore_M',
        color='sector',
        labels={'valore_M': 'Valore ($M)', 'week': 'Settimana', 'sector': 'Settore'},
        markers=True
    )
    fig_line.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        height=400
    )
    st.plotly_chart(fig_line, use_container_width=True)

else:
    # Vista singolo settore
    st.subheader(f'Andamento acquisti — {SECTOR_LABELS.get(settore_sel, settore_sel)}')

    df_time = df_filtered.copy()
    df_time['date'] = df_time['filing_date'].dt.date
    df_daily = df_time.groupby('date').agg(
        filing_count=('adsh', 'count'),
        total_value=('total_value', 'sum'),
        total_shares=('shares', 'sum')
    ).reset_index()
    df_daily['valore_M'] = df_daily['total_value'] / 1e6
    df_daily['media_7d'] = df_daily['filing_count'].rolling(7, min_periods=1).mean()

    col_left, col_right = st.columns(2)

    with col_left:
        fig_count = go.Figure()
        fig_count.add_trace(go.Bar(
            x=df_daily['date'],
            y=df_daily['filing_count'],
            name='Acquisti giornalieri',
            marker_color='#378ADD',
            opacity=0.6
        ))
        fig_count.add_trace(go.Scatter(
            x=df_daily['date'],
            y=df_daily['media_7d'],
            name='Media 7 giorni',
            line=dict(color='#1D9E75', width=2),
        ))
        fig_count.update_layout(
            title='Numero acquisti giornalieri',
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=350,
            legend=dict(orientation='h', y=1.1)
        )
        st.plotly_chart(fig_count, use_container_width=True)

    with col_right:
        fig_val = go.Figure()
        fig_val.add_trace(go.Bar(
            x=df_daily['date'],
            y=df_daily['valore_M'],
            name='Valore ($M)',
            marker_color='#BA7517',
            opacity=0.7
        ))
        fig_val.update_layout(
            title='Valore acquisti giornalieri ($M)',
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=350
        )
        st.plotly_chart(fig_val, use_container_width=True)

    # Top acquisti per valore
    st.subheader(f'Top acquisti — {settore_sel}')
    top = df_filtered.nlargest(15, 'total_value')[[
        'filing_date', 'ticker', 'company', 'insider_name',
        'insider_role', 'shares', 'price_per_share', 'total_value'
    ]].copy()
    top['filing_date'] = top['filing_date'].dt.strftime('%Y-%m-%d')
    top['total_value'] = top['total_value'].apply(lambda x: f'${x:,.0f}')
    top['shares'] = top['shares'].apply(lambda x: f'{x:,.0f}')
    top['price_per_share'] = top['price_per_share'].apply(lambda x: f'${x:.2f}')
    top.columns = ['Data', 'Ticker', 'Società', 'Insider', 'Ruolo', 'Azioni', 'Prezzo', 'Valore Tot.']
    st.dataframe(top, use_container_width=True, hide_index=True)

# --- TABELLA COMPLETA ---
st.divider()
st.subheader('Tutti gli acquisti del periodo')

df_show = df_filtered[[
    'filing_date', 'ticker', 'company', 'insider_name',
    'insider_role', 'shares', 'price_per_share', 'total_value', 'sector'
]].copy()
df_show['filing_date'] = df_show['filing_date'].dt.strftime('%Y-%m-%d')
df_show['total_value'] = df_show['total_value'].apply(lambda x: f'${x:,.0f}')
df_show['shares'] = df_show['shares'].apply(lambda x: f'{x:,.0f}')
df_show['price_per_share'] = df_show['price_per_share'].apply(lambda x: f'${x:.2f}')
df_show.columns = ['Data', 'Ticker', 'Società', 'Insider', 'Ruolo', 'Azioni', 'Prezzo', 'Valore Tot.', 'Settore']

st.dataframe(df_show, use_container_width=True, hide_index=True, height=400)

# --- MODULI FUTURI ---
st.divider()
st.header('🚧 Prossimi moduli')
col1, col2, col3 = st.columns(3)
col1.info('📉 Valutazioni settoriali\nP/E e P/S vs storico')
col2.info('🌀 Gamma Exposure\nDati opzioni CBOE')
col3.info('🌍 Macro Dashboard\nFRED: yield curve, CPI, PMI')