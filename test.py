import httpx
import xml.etree.ElementTree as ET

adsh = '0001185292-26-000003'
clean = adsh.replace('-', '')
cik_no_zeros = '1185292'

headers = {'User-Agent': 'test research@example.com'}

url_xml = f'https://www.sec.gov/Archives/edgar/data/{cik_no_zeros}/{clean}/form4-03272026_040306.xml'
r = httpx.get(url_xml, headers=headers, verify=False)
print('Status:', r.status_code)
print(r.text[:3000])
