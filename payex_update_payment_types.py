import os
import base64
import tempfile
import time
import pandas as pd
import requests
import snowflake.connector
import urllib3
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed

# Optional: suppress SSL warnings (since we're disabling verification)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- TLS Adapter that disables verification but loads client cert ---
class TLSAdapter(HTTPAdapter):
    def __init__(self, certfile, keyfile, **kwargs):
        self.certfile = certfile
        self.keyfile = keyfile
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.load_cert_chain(certfile=self.certfile, keyfile=self.keyfile)
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.load_cert_chain(certfile=self.certfile, keyfile=self.keyfile)
        kwargs['ssl_context'] = ctx
        return super().proxy_manager_for(*args, **kwargs)


def get_session():
    load_dotenv()
    token = os.getenv("PAYEX_TOKEN")
    company = os.getenv("PAYEX_COMPANY_NUMBER")
    base_url = os.getenv("PAYEX_BASE_URL")
    cert_b64 = os.getenv("PAYEX_NEW_CERTIFICATE")
    key_b64 = os.getenv("PAYEX_NEW_KEY")

    cert_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    key_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    cert_file.write(base64.b64decode(cert_b64))
    key_file.write(base64.b64decode(key_b64))
    cert_file.close()
    key_file.close()

    session = requests.Session()
    session.headers.update({'Authorization': f'Bearer {token}'})
    adapter = TLSAdapter(cert_file.name, key_file.name)
    session.mount("https://", adapter)
    return session, base_url, company, token, cert_file.name, key_file.name


def build_session(token, cert_path, key_path):
    s = requests.Session()
    s.headers.update({'Authorization': f'Bearer {token}'})
    adapter = TLSAdapter(cert_path, key_path)
    s.mount("https://", adapter)
    return s


def fetch_payment_types_serial(session, base_url, company, customer_ids):
    consent_flags = [
        "Autogiro", "Avtalegiro", "Betalingsservice",
        "RecurringCard", "RecurringInvoiceToken", "EInvoice",
        "Kivra", "EBoks"
    ]

    records = []
    for idx, cid in enumerate(customer_ids, start=1):
        url = f"{base_url}/customer/v1/{company}/customers/{cid}"
        try:
            response = session.get(url, verify=False)
            if response.status_code == 200:
                data = response.json()
                consents = data.get("activeConsents", [])
                record = (
                    int(data.get("customerNo")),
                    data.get("name"),
                    data.get("emailAddress"),
                    ",".join(consents),
                    *[(1 if c in consents else 0) for c in consent_flags],
                    data.get("legalStatus")
                )
                records.append(record)
                print(f"{idx}/{len(customer_ids)} → {cid} → Record Found")
            elif response.status_code == 404:
                print(f"{idx}/{len(customer_ids)} → {cid} → Not found (404)")
            else:
                print(f"{idx}/{len(customer_ids)} → {cid} → Status: {response.status_code}")
        except Exception as e:
            print(f"{idx}/{len(customer_ids)} → {cid} → ERROR: {e}")
        time.sleep(1.0)
    return records


def fetch_one_customer(cid, token, cert_path, key_path, base_url, company, consent_flags, delay_s):
    url = f"{base_url}/customer/v1/{company}/customers/{cid}"
    try:
        session = build_session(token, cert_path, key_path)
        resp = session.get(url, verify=False)
        if resp.status_code == 200:
            data = resp.json()
            consents = data.get("activeConsents", [])
            record = (
                int(data.get("customerNo")),
                data.get("name"),
                data.get("emailAddress"),
                ",".join(consents),
                *[(1 if c in consents else 0) for c in consent_flags],
                data.get("legalStatus")
            )
            return (cid, True, record, None)
        elif resp.status_code == 404:
            return (cid, False, None, None)
        else:
            return (cid, False, None, f"Status: {resp.status_code}")
    except Exception as e:
        return (cid, False, None, str(e))
    finally:
        if delay_s > 0:
            time.sleep(delay_s)


def fetch_payment_types_parallel(token, cert_path, key_path, base_url, company, customer_ids, max_workers, delay_s):
    consent_flags = [
        "Autogiro", "Avtalegiro", "Betalingsservice",
        "RecurringCard", "RecurringInvoiceToken", "EInvoice",
        "Kivra", "EBoks"
    ]
    records = []
    total = len(customer_ids)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                fetch_one_customer,
                cid,
                token,
                cert_path,
                key_path,
                base_url,
                company,
                consent_flags,
                delay_s,
            ): cid for cid in customer_ids
        }
        done_count = 0
        for future in as_completed(futures):
            cid = futures[future]
            done_count += 1
            try:
                _cid, ok, record, err = future.result()
                if ok and record is not None:
                    records.append(record)
                    print(f"{done_count}/{total} → {cid} → Record Found")
                elif err is None:
                    print(f"{done_count}/{total} → {cid} → Not found or no change")
                else:
                    print(f"{done_count}/{total} → {cid} → {err}")
            except Exception as e:
                print(f"{done_count}/{total} → {cid} → ERROR: {e}")
    return records


def merge_into_snowflake(records):
    if not records:
        print("No records to upsert.")
        return

    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", os.path.join(os.path.dirname(__file__), "rsa_key.p8"))
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(key_file.read(), password=None)

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key=private_key,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    cs = conn.cursor()

    try:
        print(f"Starting MERGE of {len(records)} records into PAYEX_PAYMENT_TYPES...")
        merge_start_ts = time.time()
        merge_sql = """
        MERGE INTO PAYEX_PAYMENT_TYPES tgt
        USING (
            SELECT
                %s AS customer_id,
                %s AS customer_name,
                %s AS customer_email,
                %s AS active_consents,
                %s AS is_autogiro, %s AS is_avtalegiro, %s AS is_betalingsservice,
                %s AS is_recurring_card, %s AS is_recurring_invoice_token,
                %s AS is_einvoice, %s AS is_kivra, %s AS is_eboks,
                %s AS status
        ) src
        ON tgt.customer_id = src.customer_id
        WHEN MATCHED AND (
            NVL(tgt.customer_name, '') <> NVL(src.customer_name, '') OR
            NVL(tgt.customer_email, '') <> NVL(src.customer_email, '') OR
            NVL(tgt.active_consents, '') <> NVL(src.active_consents, '') OR
            NVL(tgt.is_autogiro, 0) <> NVL(src.is_autogiro, 0) OR
            NVL(tgt.is_avtalegiro, 0) <> NVL(src.is_avtalegiro, 0) OR
            NVL(tgt.is_betalingsservice, 0) <> NVL(src.is_betalingsservice, 0) OR
            NVL(tgt.is_recurring_card, 0) <> NVL(src.is_recurring_card, 0) OR
            NVL(tgt.is_recurring_invoice_token, 0) <> NVL(src.is_recurring_invoice_token, 0) OR
            NVL(tgt.is_einvoice, 0) <> NVL(src.is_einvoice, 0) OR
            NVL(tgt.is_kivra, 0) <> NVL(src.is_kivra, 0) OR
            NVL(tgt.is_eboks, 0) <> NVL(src.is_eboks, 0) OR
            NVL(tgt.status, '') <> NVL(src.status, '')
        ) THEN UPDATE SET
            customer_name = src.customer_name,
            customer_email = src.customer_email,
            active_consents = src.active_consents,
            is_autogiro = src.is_autogiro,
            is_avtalegiro = src.is_avtalegiro,
            is_betalingsservice = src.is_betalingsservice,
            is_recurring_card = src.is_recurring_card,
            is_recurring_invoice_token = src.is_recurring_invoice_token,
            is_einvoice = src.is_einvoice,
            is_kivra = src.is_kivra,
            is_eboks = src.is_eboks,
            status = src.status
        WHEN NOT MATCHED THEN INSERT (
            customer_id, customer_name, customer_email, active_consents,
            is_autogiro, is_avtalegiro, is_betalingsservice,
            is_recurring_card, is_recurring_invoice_token,
            is_einvoice, is_kivra, is_eboks, status
        ) VALUES (
            src.customer_id, src.customer_name, src.customer_email, src.active_consents,
            src.is_autogiro, src.is_avtalegiro, src.is_betalingsservice,
            src.is_recurring_card, src.is_recurring_invoice_token,
            src.is_einvoice, src.is_kivra, src.is_eboks, src.status
        )
        """
        cs.executemany(merge_sql, records)
        conn.commit()
        merge_duration = time.time() - merge_start_ts
        print(f"Upserted {len(records)} records into PAYEX_PAYMENT_TYPES. MERGE duration: {merge_duration:.2f}s")
    finally:
        cs.close()
        conn.close()


def bulk_stage_upsert(records, mode="merge"):
    if not records:
        print("No records to upsert (stage mode).")
        return

    cols = [
        "customer_id", "customer_name", "customer_email", "active_consents",
        "is_autogiro", "is_avtalegiro", "is_betalingsservice",
        "is_recurring_card", "is_recurring_invoice_token",
        "is_einvoice", "is_kivra", "is_eboks", "status",
    ]
    df = pd.DataFrame(records, columns=cols)

    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", os.path.join(os.path.dirname(__file__), "rsa_key.p8"))
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(key_file.read(), password=None)

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key=private_key,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    cs = conn.cursor()

    try:
        stage_table = "PAYEX_PAYMENT_TYPES_STAGE"
        print(f"Staging {len(df)} records into {stage_table}…")

        cs.execute(f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} LIKE PAYEX_PAYMENT_TYPES")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            csv_path = tmp.name
            df.to_csv(csv_path, index=False)

        cs.execute(f"PUT file://{csv_path} @%{stage_table} OVERWRITE = TRUE")
        cs.execute(
            f"""
            COPY INTO {stage_table}
            FROM @%{stage_table}
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1)
            """
        )

        if mode == "replace":
            print("Running DELETE + INSERT from stage…")
            delete_sql = f"""
            DELETE FROM PAYEX_PAYMENT_TYPES tgt
            USING {stage_table} src
            WHERE tgt.customer_id = src.customer_id
            """
            cs.execute(delete_sql)

            insert_sql = f"""
            INSERT INTO PAYEX_PAYMENT_TYPES (
                customer_id, customer_name, customer_email, active_consents,
                is_autogiro, is_avtalegiro, is_betalingsservice,
                is_recurring_card, is_recurring_invoice_token,
                is_einvoice, is_kivra, is_eboks, status
            )
            SELECT {', '.join(cols)} FROM {stage_table}
            """
            cs.execute(insert_sql)
            print(f"Replaced rows for {len(df)} customers.")
        else:
            print("Running single MERGE from stage…")
            merge_sql = f"""
            MERGE INTO PAYEX_PAYMENT_TYPES tgt
            USING {stage_table} src
            ON tgt.customer_id = src.customer_id
            WHEN MATCHED AND (
                NVL(tgt.customer_name, '') <> NVL(src.customer_name, '') OR
                NVL(tgt.customer_email, '') <> NVL(src.customer_email, '') OR
                NVL(tgt.active_consents, '') <> NVL(src.active_consents, '') OR
                NVL(tgt.is_autogiro, 0) <> NVL(src.is_autogiro, 0) OR
                NVL(tgt.is_avtalegiro, 0) <> NVL(src.is_avtalegiro, 0) OR
                NVL(tgt.is_betalingsservice, 0) <> NVL(src.is_betalingsservice, 0) OR
                NVL(tgt.is_recurring_card, 0) <> NVL(src.is_recurring_card, 0) OR
                NVL(tgt.is_recurring_invoice_token, 0) <> NVL(src.is_recurring_invoice_token, 0) OR
                NVL(tgt.is_einvoice, 0) <> NVL(src.is_einvoice, 0) OR
                NVL(tgt.is_kivra, 0) <> NVL(src.is_kivra, 0) OR
                NVL(tgt.is_eboks, 0) <> NVL(src.is_eboks, 0) OR
                NVL(tgt.status, '') <> NVL(src.status, '')
            ) THEN UPDATE SET
                customer_name = src.customer_name,
                customer_email = src.customer_email,
                active_consents = src.active_consents,
                is_autogiro = src.is_autogiro,
                is_avtalegiro = src.is_avtalegiro,
                is_betalingsservice = src.is_betalingsservice,
                is_recurring_card = src.is_recurring_card,
                is_recurring_invoice_token = src.is_recurring_invoice_token,
                is_einvoice = src.is_einvoice,
                is_kivra = src.is_kivra,
                is_eboks = src.is_eboks,
                status = src.status
            WHEN NOT MATCHED THEN INSERT (
                customer_id, customer_name, customer_email, active_consents,
                is_autogiro, is_avtalegiro, is_betalingsservice,
                is_recurring_card, is_recurring_invoice_token,
                is_einvoice, is_kivra, is_eboks, status
            ) VALUES (
                src.customer_id, src.customer_name, src.customer_email, src.active_consents,
                src.is_autogiro, src.is_avtalegiro, src.is_betalingsservice,
                src.is_recurring_card, src.is_recurring_invoice_token,
                src.is_einvoice, src.is_kivra, src.is_eboks, src.status
            )
            """
            cs.execute(merge_sql)
            print(f"Merged rows for {len(df)} customers.")

        conn.commit()
    finally:
        try:
            os.remove(csv_path)
        except Exception:
            pass
        cs.close()
        conn.close()

def main():
    # Input file should contain a column named CUSTOMERID
    input_path = os.getenv("PAYEX_CUSTOMER_ID_CSV")
    if not input_path:
        raise ValueError("PAYEX_CUSTOMER_ID_CSV env var is required (path to CSV with CUSTOMERID column)")

    df = pd.read_csv(input_path)
    customer_ids = df['CUSTOMERID'].dropna().astype(str).tolist()

    # Batching via env vars
    start_idx = int(os.getenv("PAYEX_BATCH_START", "77000"))
    batch_size = int(os.getenv("PAYEX_BATCH_SIZE", "5000"))
    end_idx = min(start_idx + batch_size, len(customer_ids))
    batch_ids = customer_ids[start_idx:end_idx]

    print(f"Loaded {len(customer_ids)} customer IDs from {input_path}.")
    print(f"Processing indices {start_idx}:{end_idx} (batch size {len(batch_ids)}).")

    session, base_url, company, token, cert_path, key_path = get_session()

    max_workers = int(os.getenv("PAYEX_MAX_WORKERS", "10"))
    request_delay = float(os.getenv("PAYEX_REQUEST_DELAY", "0"))

    if max_workers > 1:
        records = fetch_payment_types_parallel(
            token, cert_path, key_path, base_url, company, batch_ids, max_workers, request_delay
        )
    else:
        records = fetch_payment_types_serial(session, base_url, company, batch_ids)

    upsert_mode = os.getenv("PAYEX_UPSERT_MODE", "merge").lower()  # merge|replace|rowmerge
    if upsert_mode in ("merge", "replace"):
        bulk_stage_upsert(records, mode=upsert_mode)
    else:
        merge_into_snowflake(records)


if __name__ == "__main__":
    main()


