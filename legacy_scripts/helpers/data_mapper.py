def map_interactions(data, campaign_id):
    interactions = {}

    def ensure(email):
        if email not in interactions:
            interactions[email] = {
                "email": email,
                "campaignId": campaign_id,
                "sent": False,
                "hardBounce": False,
                "softBounce": False,
                "uniqueOpen": False,
                "totalClicks": 0,
                "uniqueClicker": False,
                "unsubscribe": False,
            }
        return interactions[email]

    for r in data.get("sent", []):
        u = ensure(r.get("emailaddress"))
        u["sent"] = True

    for r in data.get("opens", []):
        u = ensure(r.get("emailaddress"))
        u["uniqueOpen"] = True

    for r in data.get("clicks", []):
        u = ensure(r.get("emailaddress"))
        u["totalClicks"] += 1
        u["uniqueClicker"] = True

    for r in data.get("bounces", []):
        u = ensure(r.get("emailaddress"))
        if r.get("bouncetype") == "hard":
            u["hardBounce"] = True
        else:
            u["softBounce"] = True

    for r in data.get("unsubscribes", []):
        u = ensure(r.get("emailaddress"))
        u["unsubscribe"] = True

    return list(interactions.values())
