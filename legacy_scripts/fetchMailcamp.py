import os
import sys
from datetime import datetime

import pymysql
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from helpers.mailcamp_queries import MAILCAMP_QUERIES  # noqa: E402
from helpers.data_mapper import map_interactions  # noqa: E402


MAILCAMP_ACCOUNTS = [
    {
        "name": "Travelwhale NL",
        "accountId": 1,
        "db": {
            "host": os.getenv("MAILCAMP_DB_HOST"),
            "user": os.getenv("MAILCAMP_DB_USER"),
            "password": os.getenv("MAILCAMP_DB_PASS"),
            "database": os.getenv("MAILCAMP_DB_NAME_TW_NL"),
        },
    },
    {
        "name": "Favotrip NL",
        "accountId": 2,
        "db": {
            "host": os.getenv("MAILCAMP_DB_HOST"),
            "user": os.getenv("MAILCAMP_DB_USER"),
            "password": os.getenv("MAILCAMP_DB_PASS"),
            "database": os.getenv("MAILCAMP_DB_NAME_FV_NL"),
        },
    },
    {
        "name": "Travelwhale DE",
        "accountId": 3,
        "db": {
            "host": os.getenv("MAILCAMP_DB_HOST"),
            "user": os.getenv("MAILCAMP_DB_USER"),
            "password": os.getenv("MAILCAMP_DB_PASS"),
            "database": os.getenv("MAILCAMP_DB_NAME_TW_DE"),
        },
    },
    {
        "name": "Travelwhale FR",
        "accountId": 4,
        "db": {
            "host": os.getenv("MAILCAMP_DB_HOST"),
            "user": os.getenv("MAILCAMP_DB_USER"),
            "password": os.getenv("MAILCAMP_DB_PASS"),
            "database": os.getenv("MAILCAMP_DB_NAME_TW_FR"),
        },
    },
    {
        "name": "Travelwhale UK",
        "accountId": 5,
        "db": {
            "host": os.getenv("MAILCAMP_DB_HOST"),
            "user": os.getenv("MAILCAMP_DB_USER"),
            "password": os.getenv("MAILCAMP_DB_PASS"),
            "database": os.getenv("MAILCAMP_DB_NAME_TW_UK"),
        },
    },
    {
        "name": "Travelwhale DK",
        "accountId": 6,
        "db": {
            "host": os.getenv("MAILCAMP_DB_HOST"),
            "user": os.getenv("MAILCAMP_DB_USER"),
            "password": os.getenv("MAILCAMP_DB_PASS"),
            "database": os.getenv("MAILCAMP_DB_NAME_TW_DK"),
        },
    },
]


def extract_domain(email):
    if not email or not isinstance(email, str):
        return ""
    trimmed = email.strip().lower()
    if "@" not in trimmed:
        return ""
    return trimmed.split("@", 1)[1].strip()


def build_domain_to_cluster_map(clusters):
    mapping = {}
    for cl in clusters:
        cluster_id = int(cl["id"])
        domains = cl.get("domains") or []
        for d in domains:
            if not d:
                continue
            mapping[str(d).lower()] = cluster_id
    return mapping


def parse_mailcamp_timestamp(value):
    if value is None:
        return None
    try:
        return datetime.utcfromtimestamp(int(value))
    except (TypeError, ValueError, OSError):
        return None


def get_pg_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "basegrow_prod"),
        user=os.getenv("POSTGRES_USER", "basegrowuser"),
        password=os.getenv("POSTGRES_PASSWORD", "5114"),
        host=os.getenv("POSTGRES_HOST", "82.25.97.51"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )


def ensure_others_cluster(pg_cursor, account_id):
    pg_cursor.execute(
        'SELECT "id", "domains", "name" FROM "Cluster" WHERE "accountId" = %s AND "name" = %s',
        (account_id, "Others"),
    )
    row = pg_cursor.fetchone()
    if row:
        return row

    pg_cursor.execute(
        'INSERT INTO "Cluster" ("accountId","name","domains","createdAt","updatedAt") '
        "VALUES (%s,%s,%s,NOW(),NOW()) RETURNING id, domains, name",
        (account_id, "Others", []),
    )
    return pg_cursor.fetchone()


def sync_mailcamp_account(account, pg_conn):
    name = account["name"]
    account_id = int(account["accountId"])
    db_cfg = account["db"]

    print("\nSyncing {} (accountId={})".format(name, account_id))

    mailcamp = pymysql.connect(
        host=db_cfg["host"],
        user=db_cfg["user"],
        password=db_cfg["password"],
        database=db_cfg["database"],
        cursorclass=pymysql.cursors.DictCursor,
    )
    print("Connected to MailCamp DB for {}".format(name))

    with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as pg_cur:
        pg_cur.execute(
            'SELECT "id", "domains", "name" FROM "Cluster" WHERE "accountId" = %s',
            (account_id,),
        )
        clusters = pg_cur.fetchall()

        others_cluster = ensure_others_cluster(pg_cur, account_id)
        if all(cl["id"] != others_cluster["id"] for cl in clusters):
            clusters.append(others_cluster)

        domain_to_cluster = build_domain_to_cluster_map(clusters)
        print(
            "Loaded {} clusters and {} domain mappings".format(
                len(clusters), len(domain_to_cluster)
            )
        )

        with mailcamp.cursor() as mc_cur:
            mc_cur.execute(MAILCAMP_QUERIES["campaigns"])
            campaigns = mc_cur.fetchall() or []

        for c in campaigns:
            stat_id = c.get("statid")
            print("\nProcessing campaign {} ({})".format(stat_id, c.get("newslettername")))

            data = {}
            with mailcamp.cursor() as mc_cur:
                for key, query in MAILCAMP_QUERIES.items():
                    if key == "campaigns":
                        continue
                    mc_cur.execute(query, (stat_id,))
                    data[key] = mc_cur.fetchall() or []

            unique_clickers = {
                (r.get("emailaddress") or "").lower()
                for r in (data.get("clicks") or [])
                if r.get("emailaddress")
            }

            created_at = parse_mailcamp_timestamp(c.get("starttime")) or datetime.utcnow()

            pg_cur.execute(
                'INSERT INTO "Campaign" '
                '("statId","name","sent","hardBounce","softBounce","uniqueOpeners","totalClicks",'
                '"uniqueClickers","unsubscribes","complaints","createdAt","accountId") '
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                'ON CONFLICT ("statId") DO UPDATE SET '
                '"name" = EXCLUDED."name", '
                '"sent" = EXCLUDED."sent", '
                '"hardBounce" = EXCLUDED."hardBounce", '
                '"softBounce" = EXCLUDED."softBounce", '
                '"uniqueOpeners" = EXCLUDED."uniqueOpeners", '
                '"totalClicks" = EXCLUDED."totalClicks", '
                '"uniqueClickers" = EXCLUDED."uniqueClickers", '
                '"unsubscribes" = EXCLUDED."unsubscribes", '
                '"complaints" = EXCLUDED."complaints", '
                '"accountId" = EXCLUDED."accountId", '
                '"createdAt" = COALESCE("Campaign"."createdAt", EXCLUDED."createdAt") '
                'RETURNING "id"',
                (
                    stat_id,
                    c.get("newslettername") or "stat-{}".format(stat_id),
                    int(c.get("sendsize") or 0),
                    int(c.get("bouncecount_hard") or 0),
                    int(c.get("bouncecount_soft") or 0),
                    int(c.get("emailopens_unique") or 0),
                    int(c.get("linkclicks") or 0),
                    len(unique_clickers),
                    int(c.get("unsubscribecount") or 0),
                    int(c.get("fbl") or 0),
                    created_at,
                    account_id,
                ),
            )
            campaign_id = pg_cur.fetchone()["id"]

            interactions = map_interactions(data, int(campaign_id))
            if not interactions:
                print("No interactions for campaign {}".format(stat_id))
                continue

            domain_agg = {}
            cluster_agg = {}
            user_rows = []

            for it in interactions:
                email = (it.get("email") or "").strip()
                domain = extract_domain(email)
                cluster_id = domain_to_cluster.get(domain) or int(others_cluster["id"])

                sent = bool(it.get("sent"))
                hard_bounce = bool(it.get("hardBounce"))
                soft_bounce = bool(it.get("softBounce"))
                unique_open = bool(it.get("uniqueOpen"))
                total_clicks = int(it.get("totalClicks") or 0)
                unique_clicker = bool(it.get("uniqueClicker"))
                unsubscribe = bool(it.get("unsubscribe"))

                user_rows.append(
                    (
                        email,
                        int(it.get("campaignId")),
                        sent,
                        hard_bounce,
                        soft_bounce,
                        unique_open,
                        total_clicks,
                        unique_clicker,
                        unsubscribe,
                        cluster_id,
                    )
                )

                d = domain_agg.get(domain) or {
                    "domain": domain,
                    "sent": 0,
                    "open": 0,
                    "click": 0,
                    "softBounce": 0,
                    "hardBounce": 0,
                    "uniqueOpen": 0,
                    "uniqueClick": 0,
                }
                if sent:
                    d["sent"] += 1
                if hard_bounce:
                    d["hardBounce"] += 1
                if soft_bounce:
                    d["softBounce"] += 1
                if unique_open:
                    d["uniqueOpen"] += 1
                if unique_clicker:
                    d["uniqueClick"] += 1
                d["click"] += total_clicks
                domain_agg[domain] = d

                ckey = int(cluster_id)
                cstats = cluster_agg.get(ckey) or {
                    "clusterId": ckey,
                    "sent": 0,
                    "open": 0,
                    "click": 0,
                    "softBounce": 0,
                    "hardBounce": 0,
                    "uniqueOpen": 0,
                    "uniqueClick": 0,
                }
                if sent:
                    cstats["sent"] += 1
                if hard_bounce:
                    cstats["hardBounce"] += 1
                if soft_bounce:
                    cstats["softBounce"] += 1
                if unique_open:
                    cstats["uniqueOpen"] += 1
                if unique_clicker:
                    cstats["uniqueClick"] += 1
                cstats["click"] += total_clicks
                cluster_agg[ckey] = cstats

            if user_rows:
                print("Upserting {} user interactions...".format(len(user_rows)))
                psycopg2.extras.execute_values(
                    pg_cur,
                    'INSERT INTO "UserInteraction" '
                    '("email","campaignId","sent","hardBounce","softBounce","uniqueOpen",'
                    '"totalClicks","uniqueClicker","unsubscribe","clusterId","createdAt") VALUES %s '
                    'ON CONFLICT ("campaignId","email") DO UPDATE SET '
                    '"sent" = EXCLUDED."sent", '
                    '"hardBounce" = EXCLUDED."hardBounce", '
                    '"softBounce" = EXCLUDED."softBounce", '
                    '"uniqueOpen" = EXCLUDED."uniqueOpen", '
                    '"totalClicks" = EXCLUDED."totalClicks", '
                    '"uniqueClicker" = EXCLUDED."uniqueClicker", '
                    '"unsubscribe" = EXCLUDED."unsubscribe", '
                    '"clusterId" = EXCLUDED."clusterId"',
                    [
                        row + (datetime.utcnow(),)
                        for row in user_rows
                    ],
                )

            now = datetime.utcnow()
            if domain_agg:
                print("Writing {} domain summaries...".format(len(domain_agg)))
                domain_rows = [
                    (
                        int(campaign_id),
                        stats["domain"],
                        stats["sent"],
                        stats["uniqueOpen"],
                        stats["click"],
                        stats["softBounce"],
                        stats["hardBounce"],
                        stats["uniqueOpen"],
                        stats["uniqueClick"],
                        now,
                        now,
                    )
                    for stats in domain_agg.values()
                ]
                psycopg2.extras.execute_values(
                    pg_cur,
                    'INSERT INTO "DomainInteractionSummary" '
                    '("campaignId","domain","sent","open","click","softBounce","hardBounce",'
                    '"uniqueOpen","uniqueClick","createdAt","updatedAt") VALUES %s '
                    'ON CONFLICT ("campaignId","domain") DO UPDATE SET '
                    '"sent" = EXCLUDED."sent", '
                    '"open" = EXCLUDED."open", '
                    '"click" = EXCLUDED."click", '
                    '"softBounce" = EXCLUDED."softBounce", '
                    '"hardBounce" = EXCLUDED."hardBounce", '
                    '"uniqueOpen" = EXCLUDED."uniqueOpen", '
                    '"uniqueClick" = EXCLUDED."uniqueClick", '
                    '"updatedAt" = EXCLUDED."updatedAt"',
                    domain_rows,
                )

            if cluster_agg:
                print("Writing {} cluster summaries...".format(len(cluster_agg)))
                cluster_rows = [
                    (
                        int(campaign_id),
                        stats["clusterId"],
                        stats["sent"],
                        stats["uniqueOpen"],
                        stats["click"],
                        stats["softBounce"],
                        stats["hardBounce"],
                        stats["uniqueOpen"],
                        stats["uniqueClick"],
                        now,
                        now,
                    )
                    for stats in cluster_agg.values()
                ]
                psycopg2.extras.execute_values(
                    pg_cur,
                    'INSERT INTO "ClusterInteractionSummary" '
                    '("campaignId","clusterId","sent","open","click","softBounce","hardBounce",'
                    '"uniqueOpen","uniqueClick","createdAt","updatedAt") VALUES %s '
                    'ON CONFLICT ("campaignId","clusterId") DO UPDATE SET '
                    '"sent" = EXCLUDED."sent", '
                    '"open" = EXCLUDED."open", '
                    '"click" = EXCLUDED."click", '
                    '"softBounce" = EXCLUDED."softBounce", '
                    '"hardBounce" = EXCLUDED."hardBounce", '
                    '"uniqueOpen" = EXCLUDED."uniqueOpen", '
                    '"uniqueClick" = EXCLUDED."uniqueClick", '
                    '"updatedAt" = EXCLUDED."updatedAt"',
                    cluster_rows,
                )

            pg_conn.commit()
            print(
                "Stored {} interactions and summaries for campaign statId={}".format(
                    len(interactions), stat_id
                )
            )

    mailcamp.close()
    print("Disconnected MailCamp DB for {}".format(name))


def main():
    pg_conn = get_pg_connection()
    try:
        for account in MAILCAMP_ACCOUNTS:
            try:
                sync_mailcamp_account(account, pg_conn)
            except Exception as err:
                print("Error syncing {}: {}".format(account["name"], err))
                pg_conn.rollback()
    finally:
        pg_conn.close()
        print("All accounts processed.")


if __name__ == "__main__":
    main()
