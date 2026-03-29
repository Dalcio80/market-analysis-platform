# modules/insider_buying.py
# Fonte: SEC EDGAR API ufficiale - download incrementale con volumi
import pandas as pd
import httpx
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os
import time

SIC_TO_SECTOR = {
    # Technology (XLK)
    '7372': 'XLK', '7371': 'XLK', '3674': 'XLK', '3577': 'XLK',
    '3672': 'XLK', '3669': 'XLK', '3679': 'XLK', '7374': 'XLK',
    '3571': 'XLK', '3572': 'XLK', '3575': 'XLK', '3576': 'XLK',
    '3661': 'XLK', '3663': 'XLK', '3812': 'XLK',
    '5065': 'XLK',  # electronic parts wholesale
    # Financials (XLF)
    '6020': 'XLF', '6022': 'XLF', '6021': 'XLF', '6035': 'XLF',
    '6036': 'XLF', '6099': 'XLF', '6141': 'XLF', '6153': 'XLF',
    '6159': 'XLF', '6162': 'XLF', '6163': 'XLF', '6199': 'XLF',
    '6200': 'XLF', '6211': 'XLF', '6282': 'XLF', '6311': 'XLF',
    '6321': 'XLF', '6331': 'XLF', '6351': 'XLF', '6361': 'XLF',
    '6411': 'XLF',
    '6792': 'XLF',  # investment trusts
    '6799': 'XLF',  # investors NEC
    # Energy (XLE)
    '1311': 'XLE', '2911': 'XLE', '5171': 'XLE', '1382': 'XLE',
    '1381': 'XLE', '1389': 'XLE', '2910': 'XLE', '5172': 'XLE',
    # Health Care (XLV)
    '2836': 'XLV', '8011': 'XLV', '3841': 'XLV', '2835': 'XLV',
    '2833': 'XLV', '5047': 'XLV', '8049': 'XLV', '8062': 'XLV',
    '8071': 'XLV', '8099': 'XLV', '2830': 'XLV', '2834': 'XLV',
    '3826': 'XLV', '3827': 'XLV', '5122': 'XLV',
    # Industrials (XLI)
    '3720': 'XLI', '3559': 'XLI', '3714': 'XLI', '3585': 'XLI',
    '3490': 'XLI', '3510': 'XLI', '3523': 'XLI', '3530': 'XLI',
    '3531': 'XLI', '3537': 'XLI', '3560': 'XLI', '3562': 'XLI',
    '3564': 'XLI', '3567': 'XLI', '3569': 'XLI', '3590': 'XLI',
    '3620': 'XLI', '3621': 'XLI', '3630': 'XLI', '3634': 'XLI',
    '3711': 'XLI', '3713': 'XLI', '3716': 'XLI', '3724': 'XLI',
    '3728': 'XLI', '3730': 'XLI', '3743': 'XLI', '3760': 'XLI',
    '3820': 'XLI', '3823': 'XLI', '3824': 'XLI', '3825': 'XLI',
    '4210': 'XLI', '4213': 'XLI', '4215': 'XLI', '4400': 'XLI',
    '4412': 'XLI', '4512': 'XLI', '4522': 'XLI', '4581': 'XLI',
    '7389': 'XLI', '8742': 'XLI',
    '7363': 'XLI',  # staffing/temp services
    '1600': 'XLI',  # heavy construction
    '1500': 'XLI',  # building construction
    '1520': 'XLI',  # residential construction
    '1531': 'XLI',  # operative builders
    '1540': 'XLI',  # industrial construction
    '1731': 'XLI',  # electrical work
    '7340': 'XLI',  # facility services
    # Communication Services (XLC)
    '4813': 'XLC', '4833': 'XLC', '7812': 'XLC', '4899': 'XLC',
    '4841': 'XLC', '7375': 'XLC', '4822': 'XLC', '7374': 'XLC',
    '7379': 'XLC',
    # Consumer Discretionary (XLY)
    '5940': 'XLY', '7011': 'XLY', '5571': 'XLY', '5651': 'XLY',
    '5731': 'XLY', '5511': 'XLY', '5521': 'XLY', '5531': 'XLY',
    '5541': 'XLY', '5600': 'XLY', '5621': 'XLY', '5641': 'XLY',
    '5661': 'XLY', '5700': 'XLY', '5712': 'XLY', '5734': 'XLY',
    '5812': 'XLY', '5900': 'XLY', '5944': 'XLY', '5945': 'XLY',
    '7041': 'XLY', '7900': 'XLY', '7948': 'XLY', '7990': 'XLY',
    # Consumer Staples (XLP)
    '2000': 'XLP', '2100': 'XLP', '5400': 'XLP', '2080': 'XLP',
    '2090': 'XLP', '2110': 'XLP', '2111': 'XLP', '2200': 'XLP',
    '2600': 'XLP', '2650': 'XLP', '2670': 'XLP', '2750': 'XLP',
    '2760': 'XLP', '2761': 'XLP', '5140': 'XLP', '5141': 'XLP',
    '5150': 'XLP', '5160': 'XLP', '5180': 'XLP', '5190': 'XLP',
    '5410': 'XLP', '5411': 'XLP', '5420': 'XLP', '5430': 'XLP',
    '5440': 'XLP', '5450': 'XLP', '5460': 'XLP', '5490': 'XLP',
    '5912': 'XLP', '5920': 'XLP', '5921': 'XLP',
    '2211': 'XLP',  # textile/fabric mills
    # Materials (XLB)
    '2819': 'XLB', '2821': 'XLB', '2860': 'XLB', '1094': 'XLB',
    '3350': 'XLB', '2810': 'XLB', '2820': 'XLB', '2822': 'XLB',
    '2823': 'XLB', '2824': 'XLB', '2840': 'XLB', '2842': 'XLB',
    '2843': 'XLB', '2844': 'XLB', '2850': 'XLB', '2851': 'XLB',
    '2870': 'XLB', '2890': 'XLB', '2891': 'XLB',
    '3210': 'XLB', '3211': 'XLB', '3220': 'XLB', '3229': 'XLB',
    '3240': 'XLB', '3241': 'XLB', '3250': 'XLB', '3251': 'XLB',
    '3255': 'XLB', '3260': 'XLB', '3290': 'XLB', '3310': 'XLB',
    '3312': 'XLB', '3316': 'XLB', '3317': 'XLB', '3320': 'XLB',
    '3321': 'XLB', '3330': 'XLB', '3334': 'XLB', '3339': 'XLB',
    '3340': 'XLB', '3341': 'XLB', '3353': 'XLB', '3356': 'XLB',
    '3357': 'XLB', '3360': 'XLB', '3390': 'XLB',
    '1040': 'XLB', '1090': 'XLB', '1400': 'XLB',
    # Real Estate (XLRE)
    '6500': 'XLRE', '6552': 'XLRE', '6512': 'XLRE', '6798': 'XLRE',
    '6510': 'XLRE', '6513': 'XLRE', '6514': 'XLRE', '6515': 'XLRE',
    '6519': 'XLRE', '6531': 'XLRE', '6532': 'XLRE', '6540': 'XLRE',
    '6541': 'XLRE', '6553': 'XLRE', '6726': 'XLRE',
    # Utilities (XLU)
    '4911': 'XLU', '4941': 'XLU', '4924': 'XLU', '4931': 'XLU',
    '4932': 'XLU', '4939': 'XLU', '4950': 'XLU', '4952': 'XLU',
    '4953': 'XLU', '4959': 'XLU', '4961': 'XLU', '4991': 'XLU',
}

DATA_PATH = 'data/insider_latest.csv'
HEADERS = {'User-Agent': 'MarketAnalysis research@example.com'}


def get_start_date(days_back_default=90):
    if os.path.exists(DATA_PATH):
        try:
            df_existing = pd.read_csv(DATA_PATH)
            if not df_existing.empty and 'filing_date' in df_existing.columns:
                last_date = pd.to_datetime(df_existing['filing_date']).max()
                start = last_date - timedelta(days=1)
                print(f'Download incrementale: dal {start.strftime("%Y-%m-%d")}')
                return start.strftime('%Y-%m-%d')
        except Exception as e:
            print(f'Errore lettura CSV esistente: {e}')
    start = (datetime.now() - timedelta(days=days_back_default)).strftime('%Y-%m-%d')
    print(f'Download iniziale: ultimi {days_back_default} giorni (dal {start})')
    return start


def fetch_index(date_from, date_to):
    all_hits = []
    start = 0
    size = 100

    with httpx.Client(verify=False, follow_redirects=True, timeout=30) as client:
        while True:
            url = (
                'https://efts.sec.gov/LATEST/search-index?'
                f'forms=4&dateRange=custom&startdt={date_from}&enddt={date_to}&'
                f'from={start}&size={size}'
            )
            try:
                r = client.get(url, headers=HEADERS)
                data = r.json()
                hits = data.get('hits', {}).get('hits', [])
                if not hits:
                    break

                for hit in hits:
                    src = hit.get('_source', {})
                    ciks = src.get('ciks', [])
                    names = src.get('display_names', [])
                    sics = src.get('sics', [])

                    # Secondo CIK/nome = azienda emittente
                    if len(ciks) >= 2:
                        cik_padded = ciks[1]
                        raw_name = names[1] if len(names) >= 2 else ''
                    else:
                        cik_padded = ciks[0] if ciks else ''
                        raw_name = names[0] if names else ''

                    # CIK del filer (primo) per costruire l'URL del filing
                    cik_filer = ciks[0] if ciks else ''

                    company = raw_name.split('(CIK')[0].strip()
                    sic = str(sics[0]) if sics else ''
                    sector = SIC_TO_SECTOR.get(sic, 'Other')

                    all_hits.append({
                        'adsh': src.get('adsh', ''),
                        'cik_issuer': cik_padded,
                        'cik_filer': cik_filer,
                        'company': company,
                        'filing_date': src.get('file_date', ''),
                        'sic': sic,
                        'sector': sector,
                    })

                if len(hits) < size:
                    break
                start += size
                if start >= 1000:
                    break

            except Exception as e:
                print(f'Errore fetch indice: {e}')
                break

    return all_hits


def get_xml_url(client, adsh, cik_filer):
    """
    Trova l'URL dell'XML del Form 4 scaricando l'indice del filing.
    """
    try:
        cik_no_zeros = str(int(cik_filer))
        clean = adsh.replace('-', '')
        url_idx = (
            f'https://www.sec.gov/Archives/edgar/data/'
            f'{cik_no_zeros}/{clean}/{adsh}-index.htm'
        )
        r = client.get(url_idx, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, 'html.parser')
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Cerca il file XML principale (non quello nella cartella xsl)
            if href.endswith('.xml') and 'xsl' not in href:
                return f'https://www.sec.gov{href}'
    except:
        pass
    return None


def parse_xml(client, xml_url):
    """
    Scarica e analizza l'XML del Form 4.
    Estrae: ticker, shares, price, valore totale, tipo transazione, ruolo insider.
    Considera solo transazioni nonDerivative con codice P (acquisto open market).
    """
    result = {
        'ticker': '',
        'transaction_code': '',
        'shares': 0.0,
        'price_per_share': 0.0,
        'total_value': 0.0,
        'insider_role': '',
        'insider_name': '',
    }

    try:
        r = client.get(xml_url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return result

        root = ET.fromstring(r.text)

        # Ticker dall'issuer
        issuer = root.find('.//issuer')
        if issuer is not None:
            result['ticker'] = (issuer.findtext('issuerTradingSymbol') or '').strip()

        # Nome e ruolo insider
        owner = root.find('.//reportingOwner')
        if owner is not None:
            result['insider_name'] = (
                owner.findtext('.//rptOwnerName') or ''
            ).strip()
            rel = owner.find('.//reportingOwnerRelationship')
            if rel is not None:
                if rel.findtext('isDirector') == '1':
                    result['insider_role'] = 'Director'
                elif rel.findtext('isOfficer') == '1':
                    title = rel.findtext('officerTitle') or 'Officer'
                    result['insider_role'] = title.strip()
                elif rel.findtext('isTenPercentOwner') == '1':
                    result['insider_role'] = '10% Owner'
                else:
                    result['insider_role'] = 'Other'

        # Transazioni non derivative — somma tutti gli acquisti P
        total_shares = 0.0
        total_value = 0.0
        prices = []
        codes = []

        for tx in root.findall('.//nonDerivativeTransaction'):
            code = tx.findtext('.//transactionCode') or ''
            codes.append(code)
            if code != 'P':
                continue
            try:
                shares = float(tx.findtext('.//transactionShares/value') or 0)
                price = float(tx.findtext('.//transactionPricePerShare/value') or 0)
                total_shares += shares
                total_value += shares * price
                if price > 0:
                    prices.append(price)
            except:
                pass

        result['transaction_code'] = 'P' if 'P' in codes else (codes[0] if codes else '')
        result['shares'] = total_shares
        result['price_per_share'] = sum(prices) / len(prices) if prices else 0.0
        result['total_value'] = total_value

    except Exception as e:
        pass

    return result


def enrich_with_xml_data(hits):
    """
    Per ogni filing scarica l'indice e poi l'XML per estrarre
    volumi, prezzi e ruolo insider.
    """
    rows = []
    total = len(hits)

    with httpx.Client(verify=False, follow_redirects=True, timeout=15) as client:
        for i, hit in enumerate(hits):
            adsh = hit.get('adsh', '')
            cik_filer = hit.get('cik_filer', '')
            cik_issuer = hit.get('cik_issuer', '')
            filing_date = hit.get('filing_date', '')
            company = hit.get('company', '')
            sic = hit.get('sic', '')
            sector = hit.get('sector', 'Other')

            xml_data = {
                'ticker': '',
                'transaction_code': '',
                'shares': 0.0,
                'price_per_share': 0.0,
                'total_value': 0.0,
                'insider_role': '',
                'insider_name': '',
            }

            if adsh and cik_filer:
                xml_url = get_xml_url(client, adsh, cik_filer)
                if xml_url:
                    xml_data = parse_xml(client, xml_url)
                time.sleep(0.1)

            rows.append({
                'filing_date': filing_date,
                'ticker': xml_data['ticker'],
                'company': company,
                'insider_name': xml_data['insider_name'],
                'insider_role': xml_data['insider_role'],
                'transaction_code': xml_data['transaction_code'],
                'shares': xml_data['shares'],
                'price_per_share': xml_data['price_per_share'],
                'total_value': xml_data['total_value'],
                'sic': sic,
                'sector': sector,
                'adsh': adsh,
                'cik_issuer': cik_issuer,
                'cik_filer': cik_filer,
            })

            if (i + 1) % 50 == 0:
                print(f'  Elaborati {i+1}/{total} filing...')

    return rows


def merge_and_save(new_rows):
    os.makedirs('data', exist_ok=True)
    df_new = pd.DataFrame(new_rows)

    if os.path.exists(DATA_PATH):
        df_existing = pd.read_csv(DATA_PATH)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['adsh'], keep='last')
        df_combined = df_combined.sort_values('filing_date', ascending=False)
    else:
        df_combined = df_new.sort_values('filing_date', ascending=False)

    df_combined.to_csv(DATA_PATH, index=False)
    print(f'Salvato: {DATA_PATH} ({len(df_combined)} righe totali, {len(df_new)} nuove)')
    return df_combined


if __name__ == '__main__':
    print('=== Download Insider Buys da SEC EDGAR ===')
    print(f'Data: {datetime.now().strftime("%Y-%m-%d %H:%M")}')

    date_from = get_start_date(days_back_default=90)
    date_to = datetime.now().strftime('%Y-%m-%d')

    print(f'Periodo: {date_from} -> {date_to}')
    print('Scaricando indice filing...')
    hits = fetch_index(date_from, date_to)
    print(f'Trovati {len(hits)} filing. Scaricando dettagli XML...')

    rows = enrich_with_xml_data(hits)
    df = merge_and_save(rows)

    # Solo acquisti open market per il report
    df_buys = df[df['transaction_code'] == 'P']
    print(f'\nAcquisti open market: {len(df_buys)} su {len(df)} filing totali')
    print('\nTop 10 acquisti per valore:')
    print(df_buys[['filing_date', 'ticker', 'company', 'insider_role',
                    'shares', 'total_value', 'sector']]
          .sort_values('total_value', ascending=False)
          .head(10).to_string(index=False))
    print('\nAggregazione per settore:')
    print(df_buys.groupby('sector')['total_value'].agg(['count', 'sum'])
          .sort_values('sum', ascending=False))
