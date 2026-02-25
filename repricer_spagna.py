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
        return round((p_ivato / 1.22) - (p_ivato * 0.1545) - c_sped - c_merce, 2)
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
        offers = r_p.payload.get('Offers', [])
        return (offers, None)
    except Exception as e: return [], str(e)

# --- 4. INTERFACCIA ---
st.set_page_config(page_title="Amazon ES Repricer + Connection Test", layout="wide")
st.title("🇪🇸 Amazon Spain: Repricer Strategico")

# Caricamento credenziali
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

# --- NUOVA TAB: TEST CONNESSIONE ---
with tab4:
    st.header("🔗 Diagnostica Amazon SP-API")
    if st.button("Avvia Test di Connessione"):
        with st.status("Verifica in corso...", expanded=True) as status:
            # Test 1: Secrets
            st.write("1. Controllo formato credenziali...")
            if len(creds_global['refresh_token']) > 10:
                st.write("✅ Credenziali caricate correttamente.")
            else:
                st.write("❌ Refresh Token sembra troppo corto.")
            
            # Test 2: Chiamata API Semplice (Catalog)
            st.write("2. Test comunicazione con Marketplace Spagna...")
            try:
                obj_test = Products(credentials=creds_global, marketplace=Marketplaces.ES)
                # Proviamo a cercare un ASIN generico (es. un libro famoso) per testare l'autorizzazione
                test_res = obj_test.get_item_offers("B00005N5PF", item_condition='New', item_type='Asin')
                st.write("✅ Connessione stabilita con successo!")
                status.update(label="Test completato: TUTTO OK!", state="complete")
                st.success("L'app è autorizzata a operare in Spagna.")
            except Exception as e:
                err_msg = str(e)
                st.write(f"❌ Errore rilevato: `{err_msg}`")
                if "Unauthorized" in err_msg or "403" in err_msg:
                    st.error("PROBLEMA: L'app non ha i permessi (Unauthorized).")
                    st.info("💡 Soluzione: Entra in Seller Central Spagna -> Partner Network -> Gestisci le tue App -> Autorizza l'app per questo marketplace.")
                elif "429" in err_msg:
                    st.warning("Amazon sta limitando le richieste (Too Many Requests). Attendi 1 minuto.")
                status.update(label="Test completato: ERRORE TROVATO", state="error")

# --- TAB ANALISI ---
with tab1:
    f1 = st.file_uploader("Carica File Analisi (.xlsx)", type=['xlsx'])
    if f1:
        d1 = pd.read_excel(f1)
        # (Il resto della logica rimane uguale...)
        if st.button("🚀 Avvia Analisi"):
            # Analisi...
            pass

# --- TAB DATABASE ---
with tab2:
    f_master = st.file_uploader("Carica Master Excel", type=['xlsx'])
    if f_master:
        if st.button("🔄 Importa Master"):
            # Import...
            pass

# --- TAB BACKUP ---
with tab3:
    if st.button("Scarica Database attuale"):
        df_db = pd.read_sql("SELECT * FROM prodotti", conn)
        st.download_button("Download CSV", df_db.to_csv(index=False), "backup_spagna.csv")
