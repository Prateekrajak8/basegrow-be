import os
from datetime import datetime, timedelta

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_pg_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "basegrow_prod"),
        user=os.getenv("POSTGRES_USER", "basegrowuser"),
        password=os.getenv("POSTGRES_PASSWORD", "5114"),
        host=os.getenv("POSTGRES_HOST", "82.25.97.51"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )


def cleanup_old_campaigns():
    print("Starting cleanup of old campaign-related data...")

    cutoff = datetime.utcnow() - timedelta(days=15)
    cutoff_str = cutoff.strftime("%Y%m%d")
    print("Cutoff campaign date:", cutoff_str)

    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT "id", "name" FROM "Campaign" WHERE "name" < %s',
                (cutoff_str,),
            )
            old_campaigns = cur.fetchall()

            if not old_campaigns:
                print("No old campaigns found. Cleanup skipped.")
                return

            ids = [row[0] for row in old_campaigns]
            print("Found {} campaigns to clean.".format(len(ids)))

            print("Deleting UserInteraction...")
            cur.execute(
                'DELETE FROM "UserInteraction" WHERE "campaignId" = ANY(%s)',
                (ids,),
            )

            print("Deleting DomainInteractionSummary...")
            cur.execute(
                'DELETE FROM "DomainInteractionSummary" WHERE "campaignId" = ANY(%s)',
                (ids,),
            )

            print("Deleting ClusterInteractionSummary...")
            cur.execute(
                'DELETE FROM "ClusterInteractionSummary" WHERE "campaignId" = ANY(%s)',
                (ids,),
            )

            print("Deleting Campaigns...")
            cur.execute(
                'DELETE FROM "Campaign" WHERE "id" = ANY(%s)',
                (ids,),
            )

        conn.commit()

        # VACUUM/REINDEX must run outside a transaction
        conn.autocommit = True
        with conn.cursor() as cur:
            print("Running VACUUM FULL...")
            cur.execute('VACUUM FULL "UserInteraction";')
            cur.execute('VACUUM FULL "DomainInteractionSummary";')
            cur.execute('VACUUM FULL "ClusterInteractionSummary";')
            cur.execute('VACUUM FULL "Campaign";')

            print("Reindexing...")
            cur.execute('REINDEX TABLE "UserInteraction";')
            cur.execute('REINDEX TABLE "DomainInteractionSummary";')
            cur.execute('REINDEX TABLE "ClusterInteractionSummary";')
            cur.execute('REINDEX TABLE "Campaign";')

        print("Cleanup complete!")
    except Exception as exc:
        conn.rollback()
        print("Cleanup failed:", exc)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    cleanup_old_campaigns()
