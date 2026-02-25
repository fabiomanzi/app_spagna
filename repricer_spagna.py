import streamlit as st
import pandas as pd
import sqlite3
import time
import requests
import re
import io
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

# --- 4. FUNZIONI API (NUOVO STANDARD) ---
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
    
    # Costruzione XML
    xml_header = f'<?xml version="1.0" encoding="utf-8"?><AmazonEnvelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="amzn-envelope.xsd"><Header><DocumentVersion>1.01</DocumentVersion><MerchantIdentifier>{seller_id}</MerchantIdentifier></Header><MessageType>Price</MessageType>'
    messages = ""
    for i, item in enumerate(lista_cambiamenti):
        messages += f"<Message><MessageID>{i+1}</MessageID><Price><SKU>{item['sku']}</SKU><StandardPrice currency='EUR'>{item['price']}</StandardPrice></Price></Message>"
    full_xml = xml_header + messages + "</AmazonEnvelope>"

    try:
        # Fase 1: Creazione Documento
        doc_res = obj_feed.create_feed_document(contentType="text/xml; charset=UTF-8")
        doc_id = doc_res.payload.get("feedDocumentId")
        put_url = doc_res.payload.get("url")
        
        # Fase 2: Upload fisico dei dati
        requests.put(put_url, data=full_xml.encode('utf-8'), headers={'Content-Type': 'text/xml; charset=UTF-8'})
        
        # Fase 3: Creazione Feed finale
        res = obj_feed.create_feed(
            feedType=FeedType.POST_PRODUCT_PRICING_DATA,
            inputFeedDocumentId=doc_id
        )
        return res.payload.get("feedId"), None
    except Exception as e:
        return None, str(e)

# --- 5. INTERFACCIA STREAMLIT ---
st.set_page_config(page_title="Amazon ES Repricer Pro", layout="wide")
st.title("🇪🇸 Amazon Spain Repricer")

# Caricamento credenziali una volta sola
try:
    creds_global = dict(
        refresh_token=st.secrets["amazon_api"]["refresh_token"], 
        lwa_app_id=st.secrets["amazon_api"]["lwa_app_id"], 
        lwa_client_secret=st.secrets["amazon_api"]["lwa_client_secret"]
    )
    MIO_ID_GLOBAL = st.secrets["amazon_api"]["seller_id"]
except:
    st.error("❌ Credenziali mancanti nei Secrets!")
    st.stop()

tab1, tab2, tab3 = st.tabs(["📊 Analisi e Repricing", "⚙️ Database Master", "💾 Backup"])

with tab1:
    f1 = st.file_uploader("Carica File Analisi (.xlsx)", type=['xlsx'], key="up_anal")
    if f1:
        d1 = pd.read_excel(f1)
        d1.columns = [str(c).strip().upper() for c in d1.columns]
        c_sku = next(c for c in d1.columns if 'SKU' in c)
        c_asin = next(c for c in d1.columns if 'ASIN' in c)

        if st.button("🚀 Avvia Analisi Strategica"):
            results = []
            bar = st.progress(0)
            for i, row in d1.iterrows():
                sku_amz = str(row[c_sku]).strip()
                asin = str(row[c_asin]).strip().upper()
                molt = int(sku_amz.split("_")[-1]) if "_" in sku_amz and sku_amz.split("_")[-1].isdigit() else 1
                sku_root = sku_amz.split("_")[0]

                cursor.execute("SELECT costo, peso, nome FROM prodotti WHERE sku=?", (sku_root,))
                db_data = cursor.fetchone()
                c_base, p_id, n_db = (db_data[0], db_data[1], db_data[2]) if db_data else (0, 0, "N/D")
                
                # Scraping peso se mancante
                if p_id == 0:
                    try:
                        r_scrap = requests.get(f"https://www.amazon.es/dp/{asin}", headers={"User-Agent":"Mozilla/5.0"}, timeout=5)
                        m = re.search(r"([\d.,]+)\s*(kg|g|gramos)", r_scrap.text.lower())
                        if m:
                            v = float(m.group(1).replace(',','.'))
                            p_id = v/1000 if 'g' in m.group(2) else v
                    except: p_id = 0.5

                offers, _ = recupera_prezzi_indistruttibile(asin, creds_global)
                mio, bb = 0.0, 0.0
                if offers:
                    bb = round(float(offers[0].get('ListingPrice',{}).get('Amount',0)) + float(offers[0].get('Shipping',{}).get('Amount',0)), 2)
                    mia_o = next((o for o in offers if o.get('MyOffer') or str(o.get('SellerId')) == MIO_ID_GLOBAL), None)
                    if mia_o: mio = round(float(mia_o.get('ListingPrice',{}).get('Amount',0)) + float(mia_o.get('Shipping',{}).get('Amount',0)), 2)
                
                t_min = calcola_target_es(c_base, p_id, molt)
                m_att = calcola_margine_netto(mio, c_base, p_id, molt)

                results.append({
                    "SKU": sku_amz, "ROOT": sku_root, "ASIN": asin, "Nombre": n_db, "Peso": p_id, 
                    "Precio Actual": mio, "BB": bb, "Target Min": t_min, 
                    "Target Max": round(t_min * 1.2, 2), "Margine €": m_att
                })
                bar.progress((i+1)/len(d1))
                time.sleep(0.4)
            st.session_state['report_es'] = pd.DataFrame(results)

        if 'report_es' in st.session_state:
            df = st.session_state['report_es']
            st.subheader("🤖 Repricer Dinamico")
            
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
                    c_val = res_c[0] if res_c else 0
                    m_nuovo = calcola_margine_netto(nuovo, c_val, r['Peso'], (int(r['SKU'].split("_")[-1]) if "_" in r['SKU'] else 1))
                    proposte.append({'SKU': r['SKU'], 'Attuale': r['Precio Actual'], 'Nuovo': nuovo, 'BB': r['BB'], 'Margine Previsto €': m_nuovo})
            
            if proposte:
                st.dataframe(pd.DataFrame(proposte).style.background_gradient(subset=['Margine Previsto €'], cmap='RdYlGn'))
                if st.button("🚀 APPLICA PREZZI SU AMAZON"):
                    fid, err = applica_nuovi_prezzi([{'sku': p['SKU'], 'price': p['Nuovo']} for p in proposte], creds_global)
                    if fid: st.success(f"✅ Feed inviato! ID: {fid}")
                    else: st.error(err)
            else: st.info("Prezzi già ottimizzati.")
            st.write("### Report Dettagliato")
            st.dataframe(df)

with tab2:
    st.header("⚙️ Caricamento Listino Master")
    f_master = st.file_uploader("Scegli file Excel Master", type=['xlsx'], key="up_mast")
    if f_master:
        if st.button("🔄 Aggiorna Database"):
            try:
                df_m = pd.read_excel(f_master)
                df_m.columns = [str(c).upper().strip() for c in df_m.columns]
                m_sku = next(c for c in df_m.columns if 'SKU' in c)
                m_costo = next(c for c in df_m.columns if 'COSTO' in c)
                m_peso = next(c for c in df_m.columns if 'PESO' in c)
                m_nome = next(c for c in df_m.columns if 'NOME' in c)

                for _, r in df_m.iterrows():
                    sku_base = str(r[m_sku]).split('_')[0].strip()
                    cursor.execute("INSERT INTO prodotti (sku, costo, peso, nome) VALUES (?,?,?,?) ON CONFLICT(sku) DO UPDATE SET costo=excluded.costo, nome=excluded.nome, peso=excluded.peso", (sku_base, float(r[m_costo]), float(r[m_peso]), str(r[m_nome])))
                conn.commit()
                st.success("Database Master aggiornato!")
            except Exception as e: st.error(f"Errore: {e}")

with tab3:
    if st.button("Scarica Backup DB"):
        df_db = pd.read_sql("SELECT * FROM prodotti", conn)
        st.download_button("Download CSV", df_db.to_csv(index=False), "backup.csv")
