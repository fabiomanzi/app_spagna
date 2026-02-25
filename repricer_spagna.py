import streamlit as st
import pandas as pd
import sqlite3
import time
import requests
import re
from bs4 import BeautifulSoup
from sp_api.api import Products, Feeds
from sp_api.base import Marketplaces, FeedType

# --- 1. CONFIGURAZIONE DATABASE SPAGNA ---
conn = sqlite3.connect('amazon_spain_final.db', check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS prodotti 
               (sku TEXT PRIMARY KEY, costo REAL, peso REAL, nome TEXT)''')
conn.commit()

# --- 2. LOGICA SPEDIZIONI (TABELLA REALE SPAGNA) ---
def calcola_costo_spedizione_es(peso):
    p = float(peso) if peso > 0 else 0.5
    if p <= 1: return 8.61
    elif p <= 2: return 9.11
    elif p <= 3: return 9.99
    elif p <= 4: return 10.53
    elif p <= 5: return 11.13
    elif p <= 6: return 11.69
    elif p <= 7: return 12.25
    elif p <= 8: return 12.81
    elif p <= 9: return 13.39
    elif p <= 10: return 13.94
    elif p <= 15: return 16.91
    elif p <= 16: return 16.89
    elif p <= 20: return 19.34
    elif p <= 25: return 22.61
    elif p <= 31.5: return 27.71
    else: return 35.00

# --- 3. FORMULE COMMERCIALI ---
def calcola_margine_netto(prezzo_vendita, costo_acquisto, peso, moltiplicatore):
    try:
        p_ivato = float(prezzo_vendita)
        if p_ivato <= 0: return 0
        prezzo_netto_iva = p_ivato / 1.22
        comm_amz = p_ivato * 0.1545
        c_sped = calcola_costo_spedizione_es(peso)
        c_merce = float(costo_acquisto) * moltiplicatore
        margine = prezzo_netto_iva - comm_amz - c_sped - c_merce
        return round(margine, 2)
    except: return 0

def calcola_target_es(costo_un, peso, moltiplicatore):
    try:
        costo_tot_merce = float(costo_un) * moltiplicatore
        costo_sped = calcola_costo_spedizione_es(peso)
        ricarico_fisso = costo_tot_merce * 0.10 
        costi_fissi = costo_tot_merce + costo_sped + ricarico_fisso
        denominatore = 1 - 0.04 - (0.1545 * 1.22)
        return round((costi_fissi / denominatore) * 1.22, 2)
    except: return 0

# --- 4. FUNZIONI API ---
def recupera_prezzi_indistruttibile(asin, creds):
    obj_p = Products(credentials=creds, marketplace=Marketplaces.ES)
    for t in range(3):
        try:
            r_p = obj_p.get_item_offers(asin, item_condition='New', item_type='Asin')
            offers = r_p.payload.get('Offers', [])
            return (offers, None) if offers else ([], "No Ofertas")
        except Exception as e:
            if "429" in str(e): 
                time.sleep(5 + t*2)
                continue
            return [], f"Error: {str(e)[:15]}"
    return [], "Timeout"

def applica_nuovi_prezzi(lista_cambiamenti, creds):
    obj_feed = Feeds(credentials=creds, marketplace=Marketplaces.ES)
    seller_id = st.secrets["amazon_api"]["seller_id"]
    xml_header = f'<?xml version="1.0" encoding="utf-8"?><AmazonEnvelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="amzn-envelope.xsd"><Header><DocumentVersion>1.01</DocumentVersion><MerchantIdentifier>{seller_id}</MerchantIdentifier></Header><MessageType>Price</MessageType>'
    messages = ""
    for i, item in enumerate(lista_cambiamenti):
        messages += f"<Message><MessageID>{i+1}</MessageID><Price><SKU>{item['sku']}</SKU><StandardPrice currency='EUR'>{item['price']}</StandardPrice></Price></Message>"
    full_xml = xml_header + messages + "</AmazonEnvelope>"
    try:
        res = obj_feed.create_feed(feedType=FeedType.POST_PRODUCT_PRICING_DATA, file=full_xml, contentType="text/xml")
        return res.payload.get("feedId"), None
    except Exception as e: return None, str(e)

# --- 5. INTERFACCIA STREAMLIT ---
st.set_page_config(page_title="Amazon ES Strategic Repricer", layout="wide")
st.title("🇪🇸 Amazon Spain: Strategic Repricer & Margin Tool")

# Definizione credenziali globale per la sessione
try:
    creds_global = dict(
        refresh_token=st.secrets["amazon_api"]["refresh_token"], 
        lwa_app_id=st.secrets["amazon_api"]["lwa_app_id"], 
        lwa_client_secret=st.secrets["amazon_api"]["lwa_client_secret"]
    )
    MIO_ID_GLOBAL = st.secrets["amazon_api"]["seller_id"]
except KeyError:
    st.error("❌ Credenziali non trovate nei Secrets! Controlla le impostazioni di Streamlit Cloud.")
    st.stop()

tab1, tab2, tab3 = st.tabs(["📊 Analisi e Repricing", "⚙️ Database Master", "💾 Backup"])

with tab1:
    f1 = st.file_uploader("Carica File SKU + ASIN (.es)", type=['xlsx'], key="analis_upload")
    if f1:
        d1 = pd.read_excel(f1)
        d1.columns = [str(c).strip().upper() for c in d1.columns]
        c_sku = next(c for c in d1.columns if 'SKU' in c)
        c_asin = next(c for c in d1.columns if 'ASIN' in c)

        if st.button("🚀 Avvia Analisi Strategica"):
            results = []
            bar = st.progress(0); status = st.empty()

            for i, row in d1.iterrows():
                curr = i + 1
                status.markdown(f"🔍 Analizzando SKU: `{row[c_sku]}` ({curr}/{len(d1)})")
                sku_amz = str(row[c_sku]).strip(); asin = str(row[c_asin]).strip().upper()
                molt = int(sku_amz.split("_")[-1]) if "_" in sku_amz and sku_amz.split("_")[-1].isdigit() else 1
                sku_root = sku_amz.split("_")[0]

                cursor.execute("SELECT costo, peso, nome FROM prodotti WHERE sku=?", (sku_root,))
                db_data = cursor.fetchone()
                costo_base, peso_id, nome_db = (db_data[0], db_data[1], db_data[2]) if db_data else (0, 0, "N/D")
                
                if peso_id == 0:
                    try:
                        resp = requests.get(f"https://www.amazon.es/dp/{asin}", headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
                        match = re.search(r"([\d.,]+)\s*(kg|g|gramos)", resp.text.lower())
                        if match: 
                            val = float(match.group(1).replace(',', '.'))
                            peso_id = val/1000 if 'g' in match.group(2) else val
                    except: peso_id = 0.5

                offers, _ = recupera_prezzi_indistruttibile(asin, creds_global)
                mio, bb = 0.0, 0.0
                if offers:
                    bb = round(float(offers[0].get('ListingPrice',{}).get('Amount',0)) + float(offers[0].get('Shipping',{}).get('Amount',0)), 2)
                    mia_o = next((o for o in offers if o.get('MyOffer') or str(o.get('SellerId')) == MIO_ID_GLOBAL), None)
                    if mia_o: mio = round(float(mia_o.get('ListingPrice',{}).get('Amount',0)) + float(mia_o.get('Shipping',{}).get('Amount',0)), 2)
                
                target_min = calcola_target_es(costo_base, peso_id, molt)
                margine_attuale = calcola_margine_netto(mio, costo_base, peso_id, molt)

                results.append({
                    "SKU": sku_amz, "ROOT": sku_root, "ASIN": asin, "Nombre": nome_db, "Peso": peso_id, 
                    "Precio Actual": mio, "BB": bb, "Target Min": target_min, 
                    "Target Max": round(target_min * 1.2, 2), "Margine € (Attuale)": margine_attuale
                })
                bar.progress(curr / len(d1)); time.sleep(0.4)

            st.session_state['report_es'] = pd.DataFrame(results)

        if 'report_es' in st.session_state:
            df = st.session_state['report_es']
            st.subheader("🤖 Repricer Decisionale")
            
            proposte = []
            for _, r in df.iterrows():
                nuovo = r['Precio Actual']
                if r['BB'] > r['Target Max']: nuovo = r['Target Max']
                elif r['Target Min'] <= r['BB'] <= r['Target Max']: nuovo = r['BB']
                elif 0 < r['BB'] < r['Target Min']: nuovo = r['Target Min']
                elif r['BB'] == 0: nuovo = r['Target Max']
                
                if nuovo != r['Precio Actual'] and nuovo > 0:
                    cursor.execute("SELECT costo FROM prodotti WHERE sku=?", (r['ROOT'],))
                    res_c = cursor.fetchone()
                    c_db = res_c[0] if res_c else 0
                    molt_r = int(r['SKU'].split("_")[-1]) if "_" in r['SKU'] else 1
                    margine_nuovo = calcola_margine_netto(nuovo, c_db, r['Peso'], molt_r)
                    
                    proposte.append({
                        'SKU': r['SKU'], 'Attuale': r['Precio Actual'], 'Nuovo': nuovo, 
                        'BB': r['BB'], 'Margine Previsto €': margine_nuovo,
                        'Diff Margine': round(margine_nuovo - r['Margine € (Attuale)'], 2)
                    })
            
            if proposte:
                st.dataframe(pd.DataFrame(proposte).style.background_gradient(subset=['Margine Previsto €'], cmap='RdYlGn'))
                if st.button("🚀 INVIA NUOVI PREZZI AD AMAZON"):
                    fid, err = applica_nuovi_prezzi([{'sku': p['SKU'], 'price': p['Nuovo']} for p in proposte], creds_global)
                    if fid: st.success(f"✅ Feed inviato con successo! ID: {fid}")
                    else: st.error(f"❌ Errore: {err}")
            else:
                st.info("✅ Tutti i prezzi sono già ottimizzati.")
            st.dataframe(df)

with tab2:
    st.header("⚙️ Sincronizzazione Listino Master")
    f_master = st.file_uploader("Seleziona il file Excel Master", type=['xlsx'], key="master_file_uploader")
    if f_master:
        if st.button("🔄 Importa / Aggiorna Database"):
            try:
                df_m = pd.read_excel(f_master)
                df_m.columns = [str(c).upper().strip() for c in df_m.columns]
                m_sku = next(c for c in df_m.columns if 'SKU' in c)
                m_costo = next(c for c in df_m.columns if 'COSTO' in c)
                m_peso = next(c for c in df_m.columns if 'PESO' in c)
                m_nome = next(c for c in df_m.columns if 'NOME' in c)

                for _, r in df_m.iterrows():
                    sku_val = str(r[m_sku]).split('_')[0].strip()
                    cursor.execute("""
                        INSERT INTO prodotti (sku, costo, peso, nome) VALUES (?,?,?,?)
                        ON CONFLICT(sku) DO UPDATE SET costo=excluded.costo, nome=excluded.nome, peso=excluded.peso
                    """, (sku_val, float(r[m_costo]), float(r[m_peso]), str(r[m_nome])))
                conn.commit()
                st.success("✅ Database Master popolato correttamente!")
            except Exception as e: st.error(f"Errore: {e}")

with tab3:
    st.header("💾 Backup Database")
    if st.button("Genera file di Backup"):
        df_db = pd.read_sql("SELECT * FROM prodotti", conn)
        st.download_button("Scarica Backup CSV", df_db.to_csv(index=False), "backup_db_spagna.csv")
