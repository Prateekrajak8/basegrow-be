MAILCAMP_QUERIES = {
    "sent": """
        SELECT ls.emailaddress
        FROM email_stats_newsletters_recipients snt
        LEFT JOIN email_list_subscribers ls ON ls.subscriberid = snt.subscriberid
        WHERE snt.statid = %s;
    """,
    "opens": """
        SELECT l.emailaddress
        FROM email_list_subscribers l
        JOIN email_stats_emailopens o ON l.subscriberid = o.subscriberid
        WHERE o.statid = %s;
    """,
    "clicks": """
        SELECT l.emailaddress, lc.clicktime
        FROM email_list_subscribers l
        JOIN email_stats_linkclicks lc ON l.subscriberid = lc.subscriberid
        WHERE lc.statid = %s;
    """,
    "bounces": """
        SELECT l.emailaddress, b.bouncetype
        FROM email_list_subscribers l
        JOIN email_list_subscriber_bounces b ON l.subscriberid = b.subscriberid
        WHERE b.statid = %s;
    """,
    "unsubscribes": """
        SELECT l.emailaddress
        FROM email_list_subscribers l
        JOIN email_list_subscribers_unsubscribe lsu ON l.subscriberid = lsu.subscriberid
        WHERE lsu.statid = %s;
    """,
    "campaigns": """
        SELECT
            snl.statid,
            sn.starttime,
            sn.finishtime,
            sn.sendsize,
            sn.bouncecount_soft,
            sn.bouncecount_hard,
            sn.unsubscribecount,
            sn.emailopens,
            sn.emailopens_unique,
            sn.linkclicks,
            sn.fbl,
            n.name AS newslettername,
            u.fullname AS sentby
        FROM email_stats_newsletter_lists snl
        JOIN email_stats_newsletters sn ON snl.statid = sn.statid
        JOIN email_newsletters n ON sn.newsletterid = n.newsletterid
        JOIN email_users u ON sn.sentby = u.userid
        WHERE sn.sendtype = 'newsletter'
          AND sn.starttime > UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 15 DAY))
          AND LOWER(n.name) NOT LIKE '%test%'
          AND sn.sendsize >= 10
        GROUP BY snl.statid;
    """,
}
