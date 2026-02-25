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

# --- 1. CONFIGURAZIONE DATABASE ---
conn = sqlite3.connect('amazon_spain_final.db', check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS prodotti 
               (sku TEXT PRIMARY KEY, costo REAL, peso REAL, nome TEXT)''')
conn.commit()

# --- 2. LOGICA SPEDIZIONI E MARGINI ---
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

def calcola_margine_netto(prezzo_vendita, costo_acquisto, peso, moltiplicatore):
    try:
        p_ivato = float(prezzo_vendita)
        if p_ivato <= 0: return 0
        prezzo_netto_iva = p_ivato / 1.22
        comm_amz = p_ivato * 0.1545
        c_sped = calcola_costo_spedizione_es(peso)
        c_merce = float(costo_acquisto) * moltiplicatore
        return round(prezzo_netto_iva - comm_amz - c_sped - c_merce, 2)
    except: return 0

def calcola_target_es(costo_un, peso, moltiplicatore):
    try:
        c_tot_merce = float(costo_un) * moltiplicatore
        c_sped = calcola_costo_spedizione_es(peso)
        costi_fissi = c_tot_merce + c_sped + (c_tot_merce * 0.10)
        den = 1 - 0.04 - (0.1545 * 1.22)
        return round((costi_fissi / den) * 1.22, 2)
    except: return 0

# --- 3. FUNZIONI API ---
def applica_nuovi_prezzi(lista_cambiamenti, creds):
    from sp_api.api import Feeds
    import io
    obj_feed = Feeds(credentials=creds, marketplace=Marketplaces.ES)
    seller_id = st.secrets["amazon_api"]["seller_id"]
    
    xml_header = f'<?xml version="1.0" encoding="utf-8"?><AmazonEnvelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="amzn-envelope.xsd"><Header><DocumentVersion>1.01</DocumentVersion><MerchantIdentifier>{seller_id}</MerchantIdentifier></Header><MessageType>Price</MessageType>'
    messages = "".join([f"<Message><MessageID>{i+1}</MessageID><Price><SKU>{item['sku']}</SKU><StandardPrice currency='EUR'>{item['price']}</StandardPrice></Price></Message>" for i, item in enumerate(lista_cambiamenti)])
    full_xml = xml_header + messages + "</AmazonEnvelope>"
    
    file_data = io.BytesIO(full_xml.encode('utf-8'))
    try:
        # Passiamo OBBLIGATORIAMENTE il parametro file e content_type
        doc_res = obj_feed.create_feed_document(file=file_data, content_type="text/xml")
        doc_id = doc_res.payload.get("feedDocumentId")
        
        res = obj_feed.create_feed(
            feed_type=FeedType.POST_PRODUCT_PRICING_DATA, 
            input_feed_document_id=doc_id
        )
        return res.payload.get("feedId"), None
    except Exception as e: 
        return None, str(e)

def recupera_prezzi_indistruttibile(asin, creds):
    obj_p = Products(credentials=creds, marketplace=Marketplaces.ES)
    try:
        r_p = obj_p.get_item_offers(asin, item_condition='New', item_type='Asin')
        return r_p.payload.get('Offers', []), None
    except Exception as e: 
        return [], str(e)

# --- 4. INTERFACCIA ---
st.set_page_config(page_title="Amazon ES Repricer Pro", layout="wide")
st.title("🇪🇸 Amazon Spain: Repricer Strategico")

try:
    creds_global = dict(
        refresh_token=st.secrets["amazon_api"]["refresh_token"], 
        lwa_app_id=st.secrets["amazon_api"]["lwa_app_id"], 
        lwa_client_secret=st.secrets["amazon_api"]["lwa_client_secret"]
    )
    MIO_ID_GLOBAL = st.secrets["amazon_api"]["seller_id"]
except:
    st.error("❌ Secrets non configurati correttamente.")
    st.stop()

tab1, tab2, tab3, tab4 = st.tabs(["📊 Analisi e Repricing", "⚙️ Database Master", "💾 Backup", "🔍 Diagnosi Avanzata"])

# --- TAB 1: ANALISI E REPRICER ---
with tab1:
    f1 = st.file_uploader("Carica File Analisi (SKU + ASIN)", type=['xlsx'], key="up_anal")
    if f1:
        d1 = pd.read_excel(f1)
        d1.columns = [str(c).strip().upper() for c in d1.columns]
        
        if st.button("🚀 Avvia Analisi Strategica"):
            results = []
            bar = st.progress(0)
            status_txt = st.empty()
            
            for i, row in d1.iterrows():
                sku_amz = str(row.get('SKU', '')).strip()
                asin = str(row.get('ASIN', '')).strip().upper()
                if not sku_amz or not asin: continue
                
                status_txt.text(f"Analisi in corso: {sku_amz}...")
                molt = int(sku_amz.split("_")[-1]) if "_" in sku_amz and sku_amz.split("_")[-1].isdigit() else 1
                sku_root = sku_amz.split("_")[0]

                cursor.execute("SELECT costo, peso, nome FROM prodotti WHERE sku=?", (sku_root,))
                db_data = cursor.fetchone()
                c_base, p_id, n_db = (db_data[0], db_data[1], db_data[2]) if db_data else (0, 0, "Non in Database")
                
                offers, _ = recupera_prezzi_indistruttibile(asin, creds_global)
                mio, bb = 0.0, 0.0
                if offers:
                    bb = round(float(offers[0].get('ListingPrice',{}).get('Amount',0)) + float(offers[0].get('Shipping',{}).get('Amount',0)), 2)
                    mia_o = next((o for o in offers if str(o.get('SellerId')) == MIO_ID_GLOBAL or o.get('MyOffer')), None)
                    if mia_o: mio = round(float(mia_o.get('ListingPrice',{}).get('Amount',0)) + float(mia_o.get('Shipping',{}).get('Amount',0)), 2)
                
                t_min = calcola_target_es(c_base, p_id, molt)
                m_att = calcola_margine_netto(mio, c_base, p_id, molt)
                
                results.append({
                    "SKU": sku_amz, "ROOT": sku_root, "ASIN": asin, "Nome": n_db, "Peso": p_id, 
                    "Mio Prezzo": mio, "BuyBox": bb, "Target Min": t_min, 
                    "Target Max": round(t_min * 1.25, 2), "Margine €": m_att
                })
                bar.progress((i+1)/len(d1))
                time.sleep(0.4)
            
            st.session_state['report_es'] = pd.DataFrame(results)
            status_txt.text("✅ Analisi completata!")

        if 'report_es' in st.session_state:
            df = st.session_state['report_es']
            st.subheader("🤖 Proposte di Variazione Prezzo")
            proposte = []
            for _, r in df.iterrows():
                nuovo = r['Mio Prezzo']
                if r['BuyBox'] > r['Target Max']: nuovo = r['Target Max']
                elif r['Target Min'] <= r['BuyBox'] <= r['Target Max']: nuovo = r['BuyBox']
                elif 0 < r['BuyBox'] < r['Target Min']: nuovo = r['Target Min']
                elif r['BuyBox'] == 0: nuovo = r['Target Max']
                
                if nuovo != r['Mio Prezzo'] and nuovo > 0:
                    cursor.execute("SELECT costo FROM prodotti WHERE sku=?", (r['ROOT'],))
                    c_res = cursor.fetchone()
                    c_val = c_res[0] if c_res else 0
                    m_nuovo = calcola_margine_netto(nuovo, c_val, r['Peso'], (int(r['SKU'].split("_")[-1]) if "_" in r['SKU'] else 1))
                    proposte.append({'SKU': r['SKU'], 'Attuale': r['Mio Prezzo'], 'Nuovo': nuovo, 'Margine Previsto €': m_nuovo})
            
            if proposte:
                st.dataframe(pd.DataFrame(proposte), use_container_width=True)
                if st.button("🚀 APPLICA E INVIA AD AMAZON"):
                    fid, err = applica_nuovi_prezzi([{'sku': p['SKU'], 'price': p['Nuovo']} for p in proposte], creds_global)
                    if fid: st.success(f"✅ Feed inviato! ID: {fid}")
                    else: st.error(f"❌ Errore durante l'invio: {err}")
            st.dataframe(df, use_container_width=True)

# --- TAB 2: DATABASE ---
with tab2:
    f_master = st.file_uploader("Carica Excel Master", type=['xlsx'], key="up_mast")
    if f_master:
        if st.button("🔄 Aggiorna Database"):
            df_m = pd.read_excel(f_master)
            df_m.columns = [str(c).upper().strip() for c in df_m.columns]
            c_sku = next(c for c in df_m.columns if 'SKU' in c)
            c_costo = next(c for c in df_m.columns if 'COSTO' in c)
            c_peso = next(c for c in df_m.columns if 'PESO' in c)
            c_nome = next(c for c in df_m.columns if 'NOME' in c)
            for _, r in df_m.iterrows():
                sku_root = str(r[c_sku]).split('_')[0].strip()
                cursor.execute("INSERT INTO prodotti (sku, costo, peso, nome) VALUES (?,?,?,?) ON CONFLICT(sku) DO UPDATE SET costo=excluded.costo, peso=excluded.peso, nome=excluded.nome", (sku_root, float(r[c_costo]), float(r[c_peso]), str(r[c_nome])))
            conn.commit()
            st.success("Database aggiornato!")

# --- TAB 3: BACKUP ---
with tab3:
    df_visual = pd.read_sql("SELECT * FROM prodotti", conn)
    st.dataframe(df_visual, use_container_width=True)
    st.download_button("Scarica Backup CSV", df_visual.to_csv(index=False), "backup.csv")

# --- TAB 4: DIAGNOSI AVANZATA ---
with tab4:
    st.header("🔍 Diagnosi Profonda dei Permessi")
    if st.button("Esegui Analisi Tecnica"):
        # Test Lettura
        try:
            obj_p = Products(credentials=creds_global, marketplace=Marketplaces.ES)
            obj_p.get_item_offers("B00005N5PF", item_condition='New', item_type='Asin')
            st.success("1. Lettura (Pricing Role): FUNZIONANTE")
        except Exception as e:
            st.error(f"1. Lettura: FALLITA - {e}")

        # Test Scrittura Reale (inviamo un file minimo)
        try:
            obj_f = Feeds(credentials=creds_global, marketplace=Marketplaces.ES)
            test_file = io.BytesIO(b"test")
            res = obj_f.create_feed_document(file=test_file, content_type="text/xml")
            st.success(f"2. Scrittura (Product Listing): FUNZIONANTE! ID: {res.payload.get('feedDocumentId')}")
            st.balloons()
        except Exception as e:
            st.error(f"2. Scrittura: FALLITA")
            st.code(str(e))
