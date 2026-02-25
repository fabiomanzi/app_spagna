import streamlit as st
import pandas as pd
import sqlite3
import time
import io
from sp_api.api import Products, Feeds
from sp_api.base import Marketplaces, FeedType

# --- 1. DATABASE ---
conn = sqlite3.connect('amazon_spain_final.db', check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS prodotti 
               (sku TEXT PRIMARY KEY, costo REAL, peso REAL, nome TEXT)''')
conn.commit()

# --- 2. LOGICA CALCOLI ---
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
    # Marketplace ID Spagna: A1RKKUPIHCS9HS
    marketplace_spagna = "A1RKKUPIHCS9HS"
    
    # Inizializziamo l'oggetto Feed forzando la regione Europa
    obj_feed = Feeds(credentials=creds, marketplace=Marketplaces.ES)
    seller_id = st.secrets["amazon_api"]["seller_id"]
    
    # Costruzione XML Price Feed
    xml_header = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<AmazonEnvelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xsi:noNamespaceSchemaLocation="amzn-envelope.xsd">'
        '<Header>'
        f'<DocumentVersion>1.01</DocumentVersion><MerchantIdentifier>{seller_id}</MerchantIdentifier>'
        '</Header>'
        '<MessageType>Price</MessageType>'
    )
    
    messages = ""
    for i, item in enumerate(lista_cambiamenti):
        messages += (
            f"<Message><MessageID>{i+1}</MessageID>"
            f"<Price><SKU>{item['sku']}</SKU>"
            f"<StandardPrice currency='EUR'>{item['price']}</StandardPrice></Price>"
            f"</Message>"
        )
    
    full_xml = xml_header + messages + "</AmazonEnvelope>"
    file_data = io.BytesIO(full_xml.encode('utf-8'))
    
    try:
        # FASE 1: Creazione Documento (Caricamento dati)
        doc_res = obj_feed.create_feed_document(file=file_data, content_type="text/xml")
        doc_id = doc_res.payload.get("feedDocumentId")
        
        # FASE 2: Invio Feed (Esecuzione)
        # Usiamo i nomi dei parametri corretti per la tua versione e specifichiamo la lista marketplace_ids
        res = obj_feed.create_feed(
            feed_type=FeedType.POST_PRODUCT_PRICING_DATA,
            input_feed_document_id=doc_id,
            marketplace_ids=[marketplace_spagna] 
        )
        return res.payload.get("feedId"), None
    except Exception as e:
        return None, f"Dettaglio Tecnico: {str(e)}"

def recupera_prezzi_es(asin, creds):
    obj_p = Products(credentials=creds, marketplace=Marketplaces.ES)
    try:
        r_p = obj_p.get_item_offers(asin, item_condition='New', item_type='Asin')
        return r_p.payload.get('Offers', []), None
    except Exception as e: return [], str(e)

# --- 4. INTERFACCIA ---
st.set_page_config(page_title="Repricer Spagna PRO", layout="wide")
st.title("🇪🇸 Amazon Spain Repricer")

# Credenziali
try:
    creds_global = {
        "refresh_token": st.secrets["amazon_api"]["refresh_token"],
        "lwa_app_id": st.secrets["amazon_api"]["lwa_app_id"],
        "lwa_client_secret": st.secrets["amazon_api"]["lwa_client_secret"],
    }
    MIO_ID_GLOBAL = st.secrets["amazon_api"]["seller_id"]
except:
    st.error("⚠️ Verifica i Secrets (ID, Token, Client ID, Secret)")
    st.stop()

tab1, tab2, tab3, tab4 = st.tabs(["📊 Analisi e Repricing", "⚙️ Database Master", "💾 Backup", "🔍 Diagnosi"])

with tab1:
    f1 = st.file_uploader("Carica File (.xlsx)", type=['xlsx'])
    if f1:
        df_input = pd.read_excel(f1)
        df_input.columns = [str(c).upper().strip() for c in df_input.columns]
        
        if st.button("🚀 Avvia Analisi Strategica"):
            risultati = []
            bar = st.progress(0)
            for i, row in df_input.iterrows():
                sku = str(row.get('SKU', '')).strip()
                asin = str(row.get('ASIN', '')).strip().upper()
                if not sku or not asin: continue
                
                molt = int(sku.split("_")[-1]) if "_" in sku and sku.split("_")[-1].isdigit() else 1
                sku_root = sku.split("_")[0]
                cursor.execute("SELECT costo, peso FROM prodotti WHERE sku=?", (sku_root,))
                data = cursor.fetchone()
                c_base, peso = (data[0], data[1]) if data else (0, 0.5)
                
                offers, _ = recupera_prezzi_es(asin, creds_global)
                mio, bb = 0.0, 0.0
                if offers:
                    bb = round(float(offers[0].get('ListingPrice',{}).get('Amount',0)) + float(offers[0].get('Shipping',{}).get('Amount',0)), 2)
                    mia_o = next((o for o in offers if str(o.get('SellerId')) == MIO_ID_GLOBAL or o.get('MyOffer')), None)
                    if mia_o: mio = round(float(mia_o.get('ListingPrice',{}).get('Amount',0)) + float(mia_o.get('Shipping',{}).get('Amount',0)), 2)
                
                t_min = calcola_target_es(c_base, peso, molt)
                risultati.append({"SKU": sku, "ASIN": asin, "Mio": mio, "BB": bb, "Min": t_min, "Max": round(t_min*1.3, 2)})
                bar.progress((i+1)/len(df_input))
                time.sleep(0.4)
            st.session_state['es_rep'] = pd.DataFrame(risultati)

        if 'es_rep' in st.session_state:
            df = st.session_state['es_rep']
            st.dataframe(df, use_container_width=True)
            
            proposte = []
            for _, r in df.iterrows():
                nuovo = r['Mio']
                if r['BB'] > r['Max']: nuovo = r['Max']
                elif r['Min'] <= r['BB'] <= r['Max']: nuovo = r['BB']
                elif 0 < r['BB'] < r['Min']: nuovo = r['Min']
                elif r['BB'] == 0: nuovo = r['Max']
                
                if nuovo != r['Mio'] and nuovo > 0:
                    proposte.append({'sku': r['SKU'], 'price': nuovo})
            
            if proposte:
                st.write(f"Variazioni rilevate: {len(proposte)}")
                if st.button("🚀 APPLICA E INVIA PREZZI"):
                    fid, err = applica_nuovi_prezzi(proposte, creds_global)
                    if fid: st.success(f"✅ Inviato! Feed ID: {fid}")
                    else: st.error(err)

with tab2:
    f_master = st.file_uploader("Carica Excel Master", type=['xlsx'], key="mast")
    if f_master:
        if st.button("🔄 Sincronizza DB"):
            df_m = pd.read_excel(f_master)
            df_m.columns = [str(c).upper().strip() for c in df_m.columns]
            for _, r in df_m.iterrows():
                sku = str(r.get('SKU')).split('_')[0]
                cursor.execute("INSERT INTO prodotti (sku, costo, peso) VALUES (?,?,?) ON CONFLICT(sku) DO UPDATE SET costo=excluded.costo, peso=excluded.peso", (sku, float(r.get('COSTO',0)), float(r.get('PESO',0.5))))
            conn.commit()
            st.success("Database Master aggiornato!")

with tab4:
    if st.button("🔍 Diagnosi Finale"):
        try:
            obj_f = Feeds(credentials=creds_global, marketplace=Marketplaces.ES)
            res = obj_f.create_feed_document(file=io.BytesIO(b"test"), content_type="text/xml")
            st.success(f"✅ Accesso Scrittura Documento: OK (ID: {res.payload.get('feedDocumentId')})")
        except Exception as e:
            st.error(f"❌ Fallimento Diagnosi: {str(e)}")
