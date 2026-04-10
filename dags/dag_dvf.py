"""
DAG : Pipeline DVF — HDFS (raw) → PostgreSQL (curated)
Schedule : tous les jours à 6h00
"""
from __future__ import annotations
import io, logging, os, tempfile
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DVF_URL          = "https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres/20260405-002251/valeursfoncieres-2023.txt.zip"
WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER     = "root"
HDFS_RAW_PATH    = "/data/dvf/raw"
POSTGRES_CONN_ID = "dvf_postgres"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="pipeline_dvf_immobilier",
    description="ETL DVF : téléchargement → HDFS raw → PostgreSQL curated",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "immobilier", "etl", "hdfs", "postgresql"],
)
def pipeline_dvf():

    # ── Tâche 1 ───────────────────────────────────────────────────────────────
    @task(task_id="verifier_sources")
    def verifier_sources() -> dict:
        statuts = {}

        try:
            r = requests.head(DVF_URL, timeout=10, allow_redirects=True)
            statuts["dvf_api"] = r.status_code < 500
        except requests.RequestException as e:
            logger.warning("API DVF KO : %s", e)
            statuts["dvf_api"] = False

        try:
            r = requests.get(
                f"{WEBHDFS_BASE_URL}/?op=LISTSTATUS&user.name={WEBHDFS_USER}",
                timeout=10,
            )
            statuts["hdfs"] = r.status_code == 200
        except requests.RequestException as e:
            logger.warning("HDFS KO : %s", e)
            statuts["hdfs"] = False

        logger.info("dvf_api=%s  hdfs=%s", statuts["dvf_api"], statuts["hdfs"])

        if not statuts["dvf_api"]:
            raise AirflowException("API DVF inaccessible — pipeline interrompu.")
        if not statuts["hdfs"]:
            raise AirflowException("HDFS inaccessible — pipeline interrompu.")

        statuts["timestamp"] = datetime.now().isoformat()
        return statuts

    # ── Tâche 2 ───────────────────────────────────────────────────────────────
    @task(task_id="telecharger_dvf")
    def telecharger_dvf(statuts: dict) -> str:
        annee      = datetime.now().year
        local_path = os.path.join(tempfile.gettempdir(), f"dvf_{annee}.csv")

        logger.info("Téléchargement DVF %d → %s", annee, local_path)

        zip_path = local_path.replace('.csv', '.zip')
        with requests.get(DVF_URL, stream=True, timeout=300) as r:
            r.raise_for_status()
            octets, dernier_log = 0, 0
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        octets += len(chunk)
                        if octets - dernier_log >= 50 * 1024 * 1024:
                            logger.info("Téléchargé : %.1f Mo", octets / 1e6)
                            dernier_log = octets

        import zipfile
        logger.info("Décompression du ZIP...")
        with zipfile.ZipFile(zip_path, 'r') as z:
            names = z.namelist()
            logger.info("Fichiers dans le ZIP : %s", names)
            with z.open(names[0]) as src, open(local_path, 'wb') as dst:
                dst.write(src.read())
        os.remove(zip_path)

        taille = os.path.getsize(local_path)
        if taille < 1000:
            raise AirflowException(f"Fichier trop petit ({taille} o) — échec probable.")

        logger.info("Téléchargement terminé : %.1f Mo", taille / 1e6)
        return local_path

    # ── Tâche 3 ───────────────────────────────────────────────────────────────
    @task(task_id="stocker_hdfs_raw")
    def stocker_hdfs_raw(local_path: str) -> str:
        annee          = datetime.now().year
        hdfs_file_path = f"{HDFS_RAW_PATH}/dvf_{annee}.csv"

        # Créer le répertoire
        r = requests.put(
            f"{WEBHDFS_BASE_URL}{HDFS_RAW_PATH}/?op=MKDIRS&user.name={WEBHDFS_USER}",
            timeout=30,
        )
        r.raise_for_status()
        logger.info("Répertoire HDFS prêt : %s", HDFS_RAW_PATH)

        # Étape 1 — initiation upload (307)
        r_init = requests.put(
            f"{WEBHDFS_BASE_URL}{hdfs_file_path}?op=CREATE&user.name={WEBHDFS_USER}&overwrite=true",
            allow_redirects=False,
            timeout=30,
        )
        datanode_url = r_init.headers.get("Location")
        if not datanode_url:
            raise AirflowException("WebHDFS : Location header absent")

        # Étape 2 — envoi vers DataNode
        with open(local_path, "rb") as f:
            r_up = requests.put(
                datanode_url,
                data=f,
                headers={"Content-Type": "application/octet-stream"},
                timeout=600,
            )
        if r_up.status_code not in (200, 201):
            r_up.raise_for_status()

        logger.info("Upload HDFS OK : %s (HTTP %d)", hdfs_file_path, r_up.status_code)
        os.remove(local_path)
        return hdfs_file_path

    # ── Tâche 4 ───────────────────────────────────────────────────────────────
    @task(task_id="traiter_donnees")
    def traiter_donnees(hdfs_path: str) -> dict:
        # Lecture depuis HDFS
        r = requests.get(
            f"{WEBHDFS_BASE_URL}{hdfs_path}?op=OPEN&user.name={WEBHDFS_USER}",
            allow_redirects=True,
            timeout=600,
        )
        r.raise_for_status()

        # Lecture par chunks pour économiser la RAM
        chunks = []
        codes_paris = [f"7500{i}" if i < 10 else f"750{i}" for i in range(1, 21)]
        codes_paris.append("75116")
        nb_avant = 0

        for chunk in pd.read_csv(
            io.BytesIO(r.content), sep="|", dtype=str,
            low_memory=False, chunksize=50000
        ):
            nb_avant += len(chunk)

            # Normaliser colonnes
            chunk.columns = (
                chunk.columns.str.strip().str.lower()
                .str.replace(" ", "_").str.replace("'", "_")
            )

            # Filtres métier
            if "nature_mutation" in chunk.columns:
                chunk = chunk[chunk["nature_mutation"] == "Vente"]
            if "type_local" in chunk.columns:
                chunk = chunk[chunk["type_local"] == "Appartement"]
            if "code_postal" in chunk.columns:
                chunk = chunk[chunk["code_postal"].isin(codes_paris)]

            for col in ["valeur_fonciere", "surface_reelle_bati"]:
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(
                        chunk[col].str.replace(",", ".", regex=False), errors="coerce"
                    )

            chunk = chunk[chunk["surface_reelle_bati"].between(9, 500)]
            chunk = chunk[chunk["valeur_fonciere"] > 10_000]

            if not chunk.empty:
                chunks.append(chunk)

        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        logger.info("Filtrage : %d → %d lignes", nb_avant, len(df))

        if df.empty:
            return {"agregats": [], "stats_globales": {}}

        # Calcul prix au m²
        df["prix_m2"] = df["valeur_fonciere"] / df["surface_reelle_bati"]

        # Arrondissement depuis code postal
        def cp_to_arrdt(cp):
            return 16 if cp == "75116" else int(cp[-2:])

        df["arrondissement"] = df["code_postal"].apply(cp_to_arrdt)

        # Année / mois
        if "date_mutation" in df.columns:
            df["date_mutation"] = pd.to_datetime(df["date_mutation"], errors="coerce", dayfirst=True)
            df["annee"] = df["date_mutation"].dt.year.fillna(datetime.now().year).astype(int)
            df["mois"]  = df["date_mutation"].dt.month.fillna(datetime.now().month).astype(int)
        else:
            df["annee"] = datetime.now().year
            df["mois"]  = datetime.now().month

        # Agrégation par arrondissement
        agg = (
            df.groupby(["code_postal", "arrondissement"])
            .agg(
                prix_m2_moyen   = ("prix_m2", "mean"),
                prix_m2_median  = ("prix_m2", "median"),
                prix_m2_min     = ("prix_m2", "min"),
                prix_m2_max     = ("prix_m2", "max"),
                nb_transactions = ("prix_m2", "count"),
                surface_moyenne = ("surface_reelle_bati", "mean"),
            )
            .reset_index()
        )
        annee_c = int(df["annee"].mode()[0])
        mois_c  = int(df["mois"].mode()[0])
        agg["annee"] = annee_c
        agg["mois"]  = mois_c

        agregats = agg.to_dict(orient="records")
        logger.info("%d arrondissements agrégés pour %d/%d", len(agregats), mois_c, annee_c)

        stats_globales = {
            "annee": annee_c,
            "mois":  mois_c,
            "nb_transactions_total": int(len(df)),
            "prix_m2_median_paris":  float(df["prix_m2"].median()),
            "prix_m2_moyen_paris":   float(df["prix_m2"].mean()),
            "arrdt_plus_cher":       int(agg.loc[agg["prix_m2_median"].idxmax(), "arrondissement"]),
            "arrdt_moins_cher":      int(agg.loc[agg["prix_m2_median"].idxmin(), "arrondissement"]),
            "surface_mediane":       float(df["surface_reelle_bati"].median()),
        }
        logger.info("Stats Paris : %s", stats_globales)
        return {"agregats": agregats, "stats_globales": stats_globales}

    # ── Tâche 5 ───────────────────────────────────────────────────────────────
    @task(task_id="inserer_postgresql")
    def inserer_postgresql(resultats: dict) -> int:
        hook     = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        agregats = resultats.get("agregats", [])
        stats    = resultats.get("stats_globales", {})

        if not agregats:
            logger.warning("Aucun agrégat à insérer.")
            return 0

        upsert_arrdt = """
            INSERT INTO prix_m2_arrondissement
                (code_postal, arrondissement, annee, mois,
                 prix_m2_moyen, prix_m2_median, prix_m2_min, prix_m2_max,
                 nb_transactions, surface_moyenne, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (code_postal, annee, mois) DO UPDATE SET
                prix_m2_moyen   = EXCLUDED.prix_m2_moyen,
                prix_m2_median  = EXCLUDED.prix_m2_median,
                prix_m2_min     = EXCLUDED.prix_m2_min,
                prix_m2_max     = EXCLUDED.prix_m2_max,
                nb_transactions = EXCLUDED.nb_transactions,
                surface_moyenne = EXCLUDED.surface_moyenne,
                updated_at      = NOW();
        """

        conn = hook.get_conn()
        nb   = 0
        try:
            with conn:
                with conn.cursor() as cur:
                    for row in agregats:
                        cur.execute(upsert_arrdt, (
                            str(row["code_postal"]),   int(row["arrondissement"]),
                            int(row["annee"]),          int(row["mois"]),
                            float(row["prix_m2_moyen"]), float(row["prix_m2_median"]),
                            float(row["prix_m2_min"]),  float(row["prix_m2_max"]),
                            int(row["nb_transactions"]), float(row["surface_moyenne"]),
                        ))
                        nb += 1

                    if stats:
                        cur.execute("""
                            INSERT INTO stats_marche
                                (annee,mois,nb_transactions_total,prix_m2_median_paris,
                                 prix_m2_moyen_paris,arrdt_plus_cher,arrdt_moins_cher,surface_mediane)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (annee,mois) DO UPDATE SET
                                nb_transactions_total = EXCLUDED.nb_transactions_total,
                                prix_m2_median_paris  = EXCLUDED.prix_m2_median_paris,
                                prix_m2_moyen_paris   = EXCLUDED.prix_m2_moyen_paris,
                                arrdt_plus_cher       = EXCLUDED.arrdt_plus_cher,
                                arrdt_moins_cher      = EXCLUDED.arrdt_moins_cher,
                                surface_mediane       = EXCLUDED.surface_mediane,
                                date_calcul           = NOW();
                        """, (
                            int(stats.get("annee",0)),  int(stats.get("mois",0)),
                            int(stats.get("nb_transactions_total",0)),
                            float(stats.get("prix_m2_median_paris",0)),
                            float(stats.get("prix_m2_moyen_paris",0)),
                            int(stats.get("arrdt_plus_cher",0)),
                            int(stats.get("arrdt_moins_cher",0)),
                            float(stats.get("surface_mediane",0)),
                        ))
        finally:
            conn.close()

        logger.info("%d arrondissements insérés/mis à jour", nb)
        return nb

    # ── Tâche 6 ───────────────────────────────────────────────────────────────
    @task(task_id="generer_rapport")
    def generer_rapport(nb_inseres: int) -> str:
        hook  = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        annee = datetime.now().year
        mois  = datetime.now().month

        records = hook.get_records("""
            SELECT arrondissement, prix_m2_median, prix_m2_moyen,
                   nb_transactions, surface_moyenne
            FROM prix_m2_arrondissement
            WHERE annee=%s AND mois=%s
            ORDER BY prix_m2_median DESC LIMIT 20;
        """, parameters=(annee, mois))

        ordinal = {i: f"{i}e" for i in range(1, 21)}
        ordinal[1] = "1er"

        sep    = "-" * 68
        header = f"{'Arrdt':>6} | {'Médian €/m²':>12} | {'Moyen €/m²':>11} | {'Tx':>6} | {'Surface':>9}"
        lignes = ["", f"  Classement Paris — {mois:02d}/{annee}  ({nb_inseres} lignes insérées)", "", header, sep]

        for arrdt, median, moyen, nb_tx, surf in records:
            lignes.append(
                f"{ordinal.get(int(arrdt), str(arrdt)):>6} | "
                f"{int(median or 0):>12,} | {int(moyen or 0):>11,} | "
                f"{int(nb_tx or 0):>6,} | {float(surf or 0):>7.1f} m²"
            )
        lignes.append(sep)

        rapport = "\n".join(lignes)
        logger.info(rapport)
        return rapport

    # ── Enchaînement ──────────────────────────────────────────────────────────
    t1 = verifier_sources()
    t2 = telecharger_dvf(t1)
    t3 = stocker_hdfs_raw(t2)
    t4 = traiter_donnees(t3)
    t5 = inserer_postgresql(t4)
    t6 = generer_rapport(t5)

    chain(t1, t2, t3, t4, t5, t6)

pipeline_dvf()
