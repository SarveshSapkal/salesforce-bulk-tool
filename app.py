import streamlit as st
import requests
import pandas as pd
import io
import time
# from microsoft_sql_db import save_mapping, load_mapping, save_upload_history, get_upload_history, save_user, get_user, save_downloaded_data
# from bulk_delete import bulk_delete

# ===== TEMP DUMMY FUNCTIONS FOR CLOUD =====
def save_mapping(*args, **kwargs): pass
def load_mapping(*args, **kwargs): return {}
def save_upload_history(*args, **kwargs): pass
def get_upload_history(): return None
def save_user(*args, **kwargs): pass
def get_user(*args, **kwargs): return None
def save_downloaded_data(*args, **kwargs): pass

# Configration
st.set_page_config(page_title="SF Bulk Tool", layout="wide", page_icon="☁️")


def login_salesforce(client_id, client_secret, token_url):
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    res = requests.post(token_url, data=payload)

    if res.status_code == 200:
        data = res.json()
        return {
            "access_token": data["access_token"],
            "instance_url": data["instance_url"]
        }
    else:
        return {"error": res.text}


def bulk_upload_to_salesforce(instance_url, 
                              access_token, 
                              object_name, 
                              df, 
                              operation="insert", 
                              external_id=None):
    headers = {
        "Authorization": f"Bearer {access_token}", 
        "Content-Type": "application/json; charset=UTF-8",
        "Sforce-Duplicate-Rule-Header": "allowSave=true"
    }
    
   
    job_data = {
        "object": object_name, 
        "operation": operation.lower(), 
        "contentType": "CSV", 
        "lineEnding": "CRLF"
    }
    
    # 
    if operation.lower() == "upsert" and external_id:
        job_data["externalIdFieldName"] = external_id

    res = requests.post(f"{instance_url}/services/data/v59.0/jobs/ingest", json=job_data, headers=headers)
    
    if res.status_code not in [200, 201]: 
        st.error(f"Job Creation Error: {res.text}")
        return None
        
    job_id = res.json().get("id")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    requests.put(f"{instance_url}/services/data/v59.0/jobs/ingest/{job_id}/batches", 
                 data=csv_buffer.getvalue().encode('utf-8'), 
                 headers={"Authorization": f"Bearer {access_token}", 
                          "Content-Type": "text/csv"})
    
    requests.patch(f"{instance_url}/services/data/v59.0/jobs/ingest/{job_id}", 
                    json={"state": "UploadComplete"}, headers=headers)
    return job_id

def get_job_status(instance_url, access_token, job_id):
    url = f"{instance_url}/services/data/v59.0/jobs/ingest/{job_id}"
    return requests.get(url, headers={"Authorization": f"Bearer {access_token}"}).json()

def get_failed_records(instance_url, access_token, job_id):
    url = f"{instance_url}/services/data/v59.0/jobs/ingest/{job_id}/failedResults"
    res = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    return pd.read_csv(io.StringIO(res.text)) if res.status_code == 200 else None

def get_fields(instance_url, access_token, object_name):
    url = f"{instance_url}/services/data/v59.0/sobjects/{object_name}/describe"
    res = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    if res.status_code == 200:
        data = res.json()
        all_fields = sorted([f["name"] for f in data.get("fields", []) if f["createable"]])
        req_fields = sorted([f["name"] for f in data.get("fields", []) if not f["nillable"] and not f["defaultedOnCreate"] and f["createable"]])
        return all_fields, req_fields
    return [], []

@st.cache_data
def get_objects(instance_url, access_token):
    url = f"{instance_url}/services/data/v59.0/sobjects"
    res = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    return sorted([obj['name'] for obj in res.json().get('sobjects', [])]) if res.status_code == 200 else []

def fetch_salesforce_data(instance_url, 
                          access_token, 
                          object_name, 
                          fields, 
                          where_clause=None):
    query = f"SELECT {','.join(fields)} FROM {object_name}"
    if where_clause: query += f" WHERE {where_clause}"
    res = requests.get(f"{instance_url}/services/data/v59.0/query", headers={"Authorization": f"Bearer {access_token}"}, params={"q": query})
    records = res.json().get("records", [])
    for r in records: r.pop("attributes", None)
    return pd.DataFrame(records)

# Sidebar
with st.sidebar:
    st.title("🔐 Salesforce Login")

    if st.button("Login to Salesforce"):
        res = login_salesforce(
            st.secrets["CLIENT_ID"],
            st.secrets["CLIENT_SECRET"],
            st.secrets["TOKEN_URL"]
        )

        if "access_token" in res:
            st.session_state['access_token'] = res['access_token']
            st.session_state['instance_url'] = res['instance_url']
            st.success("Login Successful ✅")
        else:
            st.error("Login Failed ❌")

    if "access_token" in st.session_state:
        st.success("Connected to Salesforce ✅")

        if st.button("Logout"):
            st.session_state.clear()
            st.rerun()

# Main App
st.title("☁️ Salesforce Bulk Operation Tool")

if "access_token" in st.session_state:
    tab_up, tab_down, tab_del, tab_hist = st.tabs(["Upload Records", "Download Records", "Delete Records", "Activity Logs"])
    objects = get_objects(st.session_state['instance_url'], 
                          st.session_state['access_token'])

    with tab_up:
        operation = st.radio("Operation", ["Insert", "Upsert"], horizontal=True)
        c1, c2 = st.columns(2)
        with c1: sel_obj = st.selectbox("1. Target Object", ["-- Select --"] + objects, key="u_obj")
        if sel_obj != "-- Select --":
            all_f, req_f = get_fields(st.session_state['instance_url'], 
                                      st.session_state['access_token'], 
                                      sel_obj)
            with c2: sel_f = st.multiselect("2. Select Fields to Map", all_f, default=req_f)
            
            with st.expander("View Mandatory Fields"): st.write(", ".join(req_f) if req_f else "None")

            up_file = st.file_uploader("3. Upload CSV File", type=["csv"], key="u_file")
            if up_file:
                df = pd.read_csv(up_file)
                st.subheader("Data Preview")
                st.dataframe(df.head(3), width="content")

                st.subheader("Column Mapping")
                saved_map = load_mapping(sel_obj); final_mapping = {}
                h1, h2, h3 = st.columns([2, 2, 1])
                h1.caption("CSV Header"); h2.caption("Salesforce Field"); h3.caption("Status")

                for i, csv_col in enumerate(df.columns):
                    r1, r2, r3 = st.columns([2, 2, 1])
                    r1.markdown(f"**{csv_col}**")
                    opts = ["-- Ignore --"] + list(set(sel_f + list(saved_map.values())))
                    def_v = saved_map.get(csv_col, "-- Ignore --")
                    m_val = r2.selectbox(f"sel_{i}", opts, index=opts.index(def_v) if def_v in opts else 0, label_visibility="collapsed", key=f"map_{i}")
                    if m_val != "-- Ignore --":
                        final_mapping[csv_col] = m_val
                        r3.write("Mapped")
                    else: r3.write("Skipped")

                if st.button("Save Mapping"): save_mapping(sel_obj, final_mapping); st.toast("Saved!")

                # Select External ID from final_mapping
                ext_id_field = None
                if operation == "Upsert":
                    ext_id_field = st.selectbox("Select External ID Field", list(final_mapping.values()))

                if st.button(" Execute Bulk Upload", width="content"):
                    up_df = df.rename(columns=final_mapping)[list(final_mapping.values())].fillna('')
                    
                    
                    missing_req = [f for f in req_f if f not in final_mapping.values()]
                    if missing_req:
                        st.error(f"Missing mandatory fields: {', '.join(missing_req)}")
                    elif operation == "Upsert" and (not ext_id_field or ext_id_field not in up_df.columns):
                        st.error("Select and map an External ID field for Upsert.")
                    else:
                        start = time.time()
                        j_id = bulk_upload_to_salesforce(st.session_state['instance_url'], 
                                                         st.session_state['access_token'], 
                                                         sel_obj, 
                                                         up_df, 
                                                         operation.lower(), 
                                                         ext_id_field)
                        if j_id:
                            st.info(f" Job ID: {j_id}")
                            pb = st.progress(0); stx = st.empty()
                            while True:
                                s = get_job_status(st.session_state['instance_url'], 
                                                   st.session_state['access_token'], 
                                                   j_id)
                                state, proc, tot = s.get("state"), s.get("numberRecordsProcessed", 0), len(up_df)
                                pb.progress(min(proc/tot, 1.0) if tot > 0 else 0)
                                stx.markdown(f"**Status:** `{state}` | **Processed:** {proc:,}/{tot:,}")
                                if state in ["JobComplete", "Failed", "Aborted"]: break
                                time.sleep(2)
                            
                            st.success(f"Finished in {time.time()-start:.2f}s")
                            f_c = s.get("numberRecordsFailed", 0); s_c = proc - f_c
                            m1, m2, m3 = st.columns(3)
                            m1.metric("Status", state); 
                            m2.metric("Success ", s_c); 
                            m3.metric("Failed ", f_c, delta_color="inverse")
                            if f_c > 0:
                                failed_df = get_failed_records(
                                    st.session_state['instance_url'],
                                    st.session_state['access_token'],
                                    j_id)
                            if failed_df is not None:
                                st.subheader("❌ Failed Records Details")
                                st.dataframe(failed_df)
                                
                                st.dataframe(get_failed_records(st.session_state['instance_url'], 
                                                                st.session_state['access_token'],
                                                                j_id),width="content")
                                save_upload_history(up_file.name, sel_obj, s_c, f_c)

    with tab_down:
        st.subheader("Download Data from Salesforce")
        d_obj = st.selectbox("Select Object", ["-- Select --"] + objects, key="d_obj")
        if d_obj != "-- Select --":
            all_f_d, _ = get_fields(st.session_state['instance_url'], 
                                    st.session_state['access_token'], 
                                    d_obj)
            sel_f_d = st.multiselect("Select Fields", all_f_d)
            whr = st.text_input("Filter (SOQL WHERE clause)", placeholder="Name LIKE 'Ac%'")
            if sel_f_d and st.button("Fetch Data"):
                df_d = fetch_salesforce_data(
                    st.session_state['instance_url'],
                    st.session_state['access_token'],
                    d_obj,
                    sel_f_d,
                    whr)
                st.session_state["download_df"] = df_d
            if "download_df" in st.session_state:
                df_d = st.session_state["download_df"]
                st.success(f"Fetched {len(df_d)} records")
                st.dataframe(df_d, width="content")
                if st.button(" Save to Database"):
                    save_downloaded_data(d_obj, df_d)
                    st.success("Data saved to database successfully ")
                csv = df_d.to_csv(index=False).encode('utf-8')
                st.download_button(
        " Download CSV",
        csv,
        f"{d_obj}_data.csv",
        "text/csv")

  

    with tab_hist:
        h = get_upload_history()
        if h is not None: st.dataframe(h, width="content")
else:
    st.info(" Login via Sidebar to start.")
