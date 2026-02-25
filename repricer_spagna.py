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
    obj_feed = Feeds(credentials=creds, marketplace=Marketplaces.ES)
    seller_id = st.secrets["amazon_api"]["seller_id"]
    xml_header = f'<?xml version="1.0" encoding="utf-8"?><AmazonEnvelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="amzn-envelope.xsd"><Header><DocumentVersion>1.01</DocumentVersion><MerchantIdentifier>{seller_id}</MerchantIdentifier></Header><MessageType>Price</MessageType>'
    messages = "".join([f"<Message><MessageID>{i+1}</MessageID><Price><SKU>{item['sku']}</SKU><StandardPrice currency='EUR'>{item['price']}</StandardPrice></Price></Message>" for i, item in enumerate(lista_cambiamenti)])
    full_xml = xml_header + messages + "</AmazonEnvelope>"
    file_data = io.BytesIO(full_xml.encode('utf-8'))
    try:
        doc_res = obj_feed.create_feed_document(file=file_data, content_type="text/xml; charset=UTF-8")
        doc_id = doc_res.payload.get("feedDocumentId")
        res = obj_feed.create_feed(feed_type=FeedType.POST_PRODUCT_PRICING_DATA, input_feed_document_id=doc_id)
        return res.payload.get("feedId"), None
    except Exception as e: return None, str(e)

def recupera_prezzi_indistruttibile(asin, creds):
    obj_p = Products(credentials=creds, marketplace=Marketplaces.ES)
    try:
        r_p = obj_p.get_item_offers(asin, item_condition='New', item_type='Asin')
        return r_p.payload.get('Offers', []), None
    except Exception as e: return [], str(e)

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

tab1, tab2, tab3, tab4 = st.tabs(["📊 Analisi", "⚙️ Database", "💾 Backup", "🔗 Test Connessione"])

# --- TAB 1: ANALISI ---
with tab1:
    f1 = st.file_uploader("Carica File Analisi (SKU + ASIN)", type=['xlsx'], key="up_anal")
    if f1:
        d1 = pd.read_excel(f1)
        d1.columns = [str(c).strip().upper() for c in d1.columns]
        
        if st.button("🚀 Avvia Analisi"):
            results = []
            bar = st.progress(0)
            status_txt = st.empty()
            
            # Controllo se il DB è vuoto prima di iniziare
            cursor.execute("SELECT COUNT(*) FROM prodotti")
            if cursor.fetchone()[0] == 0:
                st.warning("⚠️ Il Database Master è vuoto! Carica prima il listino nella Tab Database.")
            
            for i, row in d1.iterrows():
                sku_amz = str(row.get('SKU', '')).strip()
                asin = str(row.get('ASIN', '')).strip().upper()
                if not sku_amz or not asin: continue
                
                status_txt.text(f"Analisi {sku_amz}...")
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
                results.append({"SKU": sku_amz, "ASIN": asin, "Nome": n_db, "Peso": p_id, "Mio Prezzo": mio, "BuyBox": bb, "Target Min": t_min, "Margine €": m_att})
                bar.progress((i+1)/len(d1))
                time.sleep(0.5)
            
            st.session_state['report_es'] = pd.DataFrame(results)
            st.success("Analisi completata!")

        if 'report_es' in st.session_state:
            st.dataframe(st.session_state['report_es'], use_container_width=True)

# --- TAB 2: DATABASE ---
with tab2:
    st.header("⚙️ Importazione Listino Master")
    st.info("Carica il file Excel con le colonne: SKU, COSTO, PESO, NOME")
    f_master = st.file_uploader("Carica Master Excel", type=['xlsx'], key="up_mast")
    if f_master:
        if st.button("🔄 Importa Master"):
            try:
                df_m = pd.read_excel(f_master)
                df_m.columns = [str(c).upper().strip() for c in df_m.columns]
                
                # Mappatura flessibile colonne
                col_sku = next(c for c in df_m.columns if 'SKU' in c)
                col_costo = next(c for c in df_m.columns if 'COSTO' in c)
                col_peso = next(c for c in df_m.columns if 'PESO' in c)
                col_nome = next(c for c in df_m.columns if 'NOME' in c)

                for _, r in df_m.iterrows():
                    sku_root = str(r[col_sku]).split('_')[0].strip()
                    cursor.execute("""INSERT INTO prodotti (sku, costo, peso, nome) VALUES (?,?,?,?) 
                                   ON CONFLICT(sku) DO UPDATE SET costo=excluded.costo, peso=excluded.peso, nome=excluded.nome""", 
                                   (sku_root, float(r[col_costo]), float(r[col_peso]), str(r[col_nome])))
                conn.commit()
                st.success(f"✅ Importati {len(df_m)} prodotti nel database!")
            except Exception as e:
                st.error(f"Errore durante l'importazione: {e}")

# --- TAB 3: BACKUP & VISUALIZZAZIONE ---
with tab3:
    st.header("💾 Gestione Dati")
    df_visual = pd.read_sql("SELECT * FROM prodotti", conn)
    st.write(f"Prodotti attualmente nel database: {len(df_visual)}")
    st.dataframe(df_visual, use_container_width=True)
    
    if st.button("Genera Backup CSV"):
        st.download_button("Scarica CSV", df_visual.to_csv(index=False), "backup_db_spagna.csv")

# --- TAB 4: TEST ---
with tab4:
    st.header("🔗 Diagnostica")
    if st.button("Avvia Test Connessione"):
        try:
            obj_test = Products(credentials=creds_global, marketplace=Marketplaces.ES)
            test_res = obj_test.get_item_offers("B00005N5PF", item_condition='New', item_type='Asin')
            st.success("✅ Connessione ad Amazon Spagna funzionante!")
        except Exception as e:
            st.error(f"❌ Errore: {e}")
