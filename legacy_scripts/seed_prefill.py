import os
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

load_dotenv()


MAILCAMP_ACCOUNTS = [
    {"id": 1, "name": "Travelwhale NL"},
    {"id": 2, "name": "Favotrip NL"},
    {"id": 3, "name": "Travelwhale DE"},
    {"id": 4, "name": "Travelwhale FR"},
    {"id": 5, "name": "Travelwhale UK"},
    {"id": 6, "name": "Travelwhale DK"},
]


CLUSTERS_BY_ACCOUNT = {
    1: [
        {
            "name": "Microsoft Cluster",
            "domains": [
                "hotmail.com",
                "hotmail.nl",
                "live.com",
                "live.nl",
                "outlook.com",
                "outlook.nl",
                "msn.com",
                "msn.nl",
                "windowslive.com",
                "hotmail.be",
                "live.be",
                "outlook.be",
            ],
        },
        {"name": "Gmail cluster", "domains": ["gmail.com", "googlemail.com"]},
        {
            "name": "KPN cluster",
            "domains": [
                "kpnmail.nl",
                "planet.nl",
                "hetnet.nl",
                "freeler.nl",
                "xs4all.nl",
                "surfmail.nl",
                "kpnplanet.nl",
                "telfort.nl",
                "tiscali.nl",
                "12move.nl",
            ],
        },
        {
            "name": "Ziggo/UPC cluster",
            "domains": [
                "ziggo.nl",
                "casema.nl",
                "chello.nl",
                "home.nl",
                "quicknet.nl",
                "upcmail.nl",
            ],
        },
        {"name": "Telenet cluster", "domains": ["telenet.be"]},
        {"name": "Skynet/Proximus cluster", "domains": ["skynet.be", "proximus.be"]},
        {
            "name": "Yahoo cluster",
            "domains": ["yahoo.com", "yahoo.fr", "ymail.com", "laposte.net"],
        },
        {"name": "Apple cluster", "domains": ["icloud.com", "me.com", "mac.com"]},
        {"name": "Pandora cluster", "domains": ["pandora.be"]},
        {"name": "ZeelandNet cluster", "domains": ["zeelandnet.nl"]},
        {"name": "Caiway cluster", "domains": ["caiway.nl", "kabelfoon.nl"]},
        {"name": "Online.nl cluster", "domains": ["online.nl"]},
        {"name": "Scarlet cluster", "domains": ["scarlet.be"]},
        {"name": "Solcon cluster", "domains": ["solcon.nl"]},
        {"name": "WXS cluster", "domains": ["wxs.nl"]},
        {"name": "HCCnet cluster", "domains": ["hccnet.nl"]},
        {"name": "OnsBrabantNet cluster", "domains": ["onsbrabantnet.nl"]},
        {"name": "Mail.com cluster", "domains": ["mail.com"]},
        {"name": "Versatel cluster", "domains": ["versatel.nl"]},
    ],
    2: [
        {
            "name": "Microsoft Cluster",
            "domains": [
                "hotmail.com",
                "hotmail.nl",
                "live.com",
                "live.nl",
                "outlook.com",
                "outlook.nl",
                "msn.com",
                "msn.nl",
                "windowslive.com",
                "hotmail.be",
                "live.be",
                "outlook.be",
            ],
        },
        {"name": "Gmail cluster", "domains": ["gmail.com", "googlemail.com"]},
        {
            "name": "KPN cluster",
            "domains": [
                "kpnmail.nl",
                "planet.nl",
                "hetnet.nl",
                "freeler.nl",
                "xs4all.nl",
                "surfmail.nl",
                "kpnplanet.nl",
                "telfort.nl",
                "tiscali.nl",
                "12move.nl",
            ],
        },
        {
            "name": "Ziggo/UPC cluster",
            "domains": [
                "ziggo.nl",
                "casema.nl",
                "chello.nl",
                "home.nl",
                "quicknet.nl",
                "upcmail.nl",
            ],
        },
        {"name": "Telenet cluster", "domains": ["telenet.be"]},
        {"name": "Skynet/Proximus cluster", "domains": ["skynet.be", "proximus.be"]},
        {
            "name": "Yahoo cluster",
            "domains": ["yahoo.com", "yahoo.fr", "ymail.com", "laposte.net"],
        },
        {"name": "Apple cluster", "domains": ["icloud.com", "me.com", "mac.com"]},
        {"name": "Pandora cluster", "domains": ["pandora.be"]},
        {"name": "ZeelandNet cluster", "domains": ["zeelandnet.nl"]},
        {"name": "Caiway cluster", "domains": ["caiway.nl", "kabelfoon.nl"]},
        {"name": "Online.nl cluster", "domains": ["online.nl"]},
        {"name": "Scarlet cluster", "domains": ["scarlet.be"]},
        {"name": "Solcon cluster", "domains": ["solcon.nl"]},
        {"name": "WXS cluster", "domains": ["wxs.nl"]},
        {"name": "HCCnet cluster", "domains": ["hccnet.nl"]},
        {"name": "OnsBrabantNet cluster", "domains": ["onsbrabantnet.nl"]},
        {"name": "Mail.com cluster", "domains": ["mail.com"]},
        {"name": "Versatel cluster", "domains": ["versatel.nl"]},
    ],
    3: [
        {
            "name": "Microsoft",
            "domains": [
                "hotmail.com",
                "hotmail.de",
                "live.com",
                "live.de",
                "msn.com",
                "outlook.com",
                "outlook.de",
            ],
        },
        {"name": "Google", "domains": ["gmail.com", "googlemail.com"]},
        {"name": "Apple", "domains": ["icloud.com", "mac.com", "me.com"]},
        {"name": "Telekom", "domains": ["t-online.de"]},
        {"name": "Yahoo", "domains": ["yahoo.com", "yahoo.de", "yahoo.fr", "ymail.com"]},
        {"name": "Vodafone", "domains": ["arcor.de", "vodafone.de"]},
        {"name": "AOL", "domains": ["aol.com", "aol.de"]},
        {"name": "GMX", "domains": ["gmx.at", "gmx.de", "gmx.net"]},
        {"name": "Web.de", "domains": ["email.de", "web.de"]},
        {"name": "Mail.de", "domains": ["mail.de"]},
        {"name": "NetCologne", "domains": ["netcologne.de"]},
        {"name": "Swisscom", "domains": ["bluewin.ch"]},
        {"name": "1&1 Ionos", "domains": ["onlinehome.de"]},
        {"name": "Mail.ru", "domains": ["mail.ru"]},
    ],
    4: [
        {
            "name": "Microsoft",
            "domains": [
                "hotmail.com",
                "hotmail.fr",
                "live.fr",
                "msn.com",
                "outlook.com",
                "outlook.fr",
            ],
        },
        {"name": "Google", "domains": ["gmail.com"]},
        {"name": "Yahoo", "domains": ["yahoo.com", "yahoo.de", "yahoo.fr", "ymail.com"]},
        {"name": "Apple", "domains": ["icloud.com", "mac.com", "me.com"]},
        {"name": "AOL", "domains": ["aol.com", "aol.fr"]},
        {"name": "Swisscom", "domains": ["bluewin.ch"]},
        {"name": "Mail.ru", "domains": ["mail.ru"]},
        {"name": "GMX", "domains": ["gmx.de", "gmx.net"]},
        {"name": "Web.de", "domains": ["web.de"]},
    ],
    5: [
        {
            "name": "Hotmail",
            "domains": [
                "hotmail.com",
                "hotmail.co.uk",
                "msn.com",
                "outlook.com",
                "live.com",
            ],
        },
        {"name": "Google", "domains": ["gmail.com", "googlemail.com"]},
        {
            "name": "BT Group",
            "domains": ["btinternet.com", "btopenworld.com", "talk21.com"],
        },
        {"name": "Apple", "domains": ["icloud.com", "mac.com", "me.com"]},
        {"name": "Sky", "domains": ["sky.com"]},
        {
            "name": "Virgin Media",
            "domains": ["blueyonder.co.uk", "ntlworld.com", "virginmedia.com"],
        },
        {"name": "Yahoo", "domains": ["yahoo.co.uk"]},
        {"name": "Mail.ru", "domains": ["mail.ru"]},
        {"name": "AOL", "domains": ["aol.com"]},
    ],
    6: [
        {
            "name": "Microsoft",
            "domains": [
                "hotmail.co.uk",
                "hotmail.com",
                "hotmail.de",
                "hotmail.dk",
                "hotmail.fr",
                "live.co.uk",
                "live.com",
                "live.dk",
                "live.fr",
                "msn.com",
                "outlook.com",
                "outlook.dk",
            ],
        },
        {"name": "Google", "domains": ["gmail.com"]},
        {
            "name": "Yahoo",
            "domains": [
                "yahoo.co.uk",
                "yahoo.com",
                "yahoo.de",
                "yahoo.dk",
                "yahoo.fr",
                "yahoo.no",
                "ymail.com",
            ],
        },
        {"name": "Apple", "domains": ["icloud.com", "mac.com", "me.com"]},
        {"name": "AOL", "domains": ["aol.com"]},
        {"name": "Mail.ru", "domains": ["mail.ru"]},
        {"name": "GMX", "domains": ["gmx.de", "gmx.net"]},
        {"name": "Web.de", "domains": ["web.de"]},
        {"name": "BT Group", "domains": ["btinternet.com"]},
        {"name": "Swisscom", "domains": ["bluewin.ch"]},
    ],
}


def get_pg_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "basegrow_prod"),
        user=os.getenv("POSTGRES_USER", "basegrowuser"),
        password=os.getenv("POSTGRES_PASSWORD", "5114"),
        host=os.getenv("POSTGRES_HOST", "82.25.97.51"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )


def seed_accounts_and_clusters():
    now = datetime.utcnow()
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            for account in MAILCAMP_ACCOUNTS:
                cur.execute(
                    'INSERT INTO "Account" ("id","name","apiKey") '
                    "VALUES (%s,%s,%s) "
                    'ON CONFLICT ("id") DO UPDATE SET '
                    '"name" = EXCLUDED."name", '
                    '"apiKey" = EXCLUDED."apiKey"',
                    (account["id"], account["name"], None),
                )

                clusters = CLUSTERS_BY_ACCOUNT.get(account["id"], [])
                for cluster in clusters:
                    cur.execute(
                        'SELECT "id" FROM "Cluster" WHERE "accountId" = %s AND "name" = %s',
                        (account["id"], cluster["name"]),
                    )
                    row = cur.fetchone()
                    if not row:
                        cur.execute(
                            'INSERT INTO "Cluster" '
                            '("accountId","name","domains","createdAt","updatedAt") '
                            "VALUES (%s,%s,%s,%s,%s)",
                            (account["id"], cluster["name"], cluster["domains"], now, now),
                        )
                    else:
                        cur.execute(
                            'UPDATE "Cluster" SET "domains" = %s, "updatedAt" = %s '
                            'WHERE "accountId" = %s AND "name" = %s',
                            (cluster["domains"], now, account["id"], cluster["name"]),
                        )

                cur.execute(
                    'SELECT "id" FROM "Cluster" WHERE "accountId" = %s AND "name" = %s',
                    (account["id"], "Others"),
                )
                row = cur.fetchone()
                if not row:
                    cur.execute(
                        'INSERT INTO "Cluster" '
                        '("accountId","name","domains","createdAt","updatedAt") '
                        "VALUES (%s,%s,%s,%s,%s)",
                        (account["id"], "Others", [], now, now),
                    )

        conn.commit()
        print("Prefill complete: accounts and clusters inserted/updated.")
    except Exception as exc:
        conn.rollback()
        print("Prefill failed:", exc)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    seed_accounts_and_clusters()
