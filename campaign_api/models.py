from django.contrib.postgres.fields import ArrayField
from django.db import models


class Account(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.TextField(unique=True)
    api_key = models.TextField(db_column="apiKey", null=True, blank=True)

    class Meta:
        managed = True
        db_table = "Account"

    def __str__(self):
        return self.name


class Campaign(models.Model):
    id = models.BigAutoField(primary_key=True)
    stat_id = models.IntegerField(db_column="statId", unique=True)
    name = models.TextField()
    sent = models.IntegerField()
    hard_bounce = models.IntegerField(db_column="hardBounce")
    soft_bounce = models.IntegerField(db_column="softBounce")
    unique_openers = models.IntegerField(db_column="uniqueOpeners")
    total_clicks = models.IntegerField(db_column="totalClicks")
    unique_clickers = models.IntegerField(db_column="uniqueClickers")
    unsubscribes = models.IntegerField()
    complaints = models.IntegerField()
    created_at = models.DateTimeField(db_column="createdAt")
    account = models.ForeignKey(
        Account,
        db_column="accountId",
        null=True,
        blank=True,
        on_delete=models.DO_NOTHING,
        related_name="campaigns",
    )

    class Meta:
        managed = True
        db_table = "Campaign"

    def __str__(self):
        return self.name


class Cluster(models.Model):
    id = models.BigAutoField(primary_key=True)
    account = models.ForeignKey(
        Account,
        db_column="accountId",
        on_delete=models.DO_NOTHING,
        related_name="clusters",
    )
    name = models.TextField()
    domains = ArrayField(base_field=models.TextField(), default=list)
    created_at = models.DateTimeField(db_column="createdAt")
    updated_at = models.DateTimeField(db_column="updatedAt")

    class Meta:
        managed = True
        db_table = "Cluster"

    def __str__(self):
        return self.name


class UserInteraction(models.Model):
    id = models.BigAutoField(primary_key=True)
    campaign = models.ForeignKey(
        Campaign,
        db_column="campaignId",
        on_delete=models.DO_NOTHING,
        related_name="interactions",
    )
    cluster = models.ForeignKey(
        Cluster,
        db_column="clusterId",
        null=True,
        blank=True,
        on_delete=models.DO_NOTHING,
        related_name="user_interactions",
    )
    email = models.TextField()
    sent = models.BooleanField()
    hard_bounce = models.BooleanField(db_column="hardBounce")
    soft_bounce = models.BooleanField(db_column="softBounce")
    unique_open = models.BooleanField(db_column="uniqueOpen")
    total_clicks = models.IntegerField(db_column="totalClicks")
    unique_clicker = models.BooleanField(db_column="uniqueClicker")
    unsubscribe = models.BooleanField(null=True, blank=True)
    created_at = models.DateTimeField(db_column="createdAt")

    class Meta:
        managed = True
        db_table = "UserInteraction"
        unique_together = (("campaign", "email"),)


class ClusterInteractionSummary(models.Model):
    id = models.BigAutoField(primary_key=True)
    campaign = models.ForeignKey(
        Campaign,
        db_column="campaignId",
        on_delete=models.DO_NOTHING,
        related_name="cluster_summaries",
    )
    cluster = models.ForeignKey(
        Cluster,
        db_column="clusterId",
        on_delete=models.DO_NOTHING,
        related_name="summaries",
    )
    sent = models.IntegerField(default=0)
    open = models.IntegerField(default=0)
    click = models.IntegerField(default=0)
    soft_bounce = models.IntegerField(db_column="softBounce", default=0)
    hard_bounce = models.IntegerField(db_column="hardBounce", default=0)
    unique_open = models.IntegerField(db_column="uniqueOpen", default=0)
    unique_click = models.IntegerField(db_column="uniqueClick", default=0)
    created_at = models.DateTimeField(db_column="createdAt")
    updated_at = models.DateTimeField(db_column="updatedAt")

    class Meta:
        managed = True
        db_table = "ClusterInteractionSummary"
        unique_together = (("campaign", "cluster"),)


class DomainInteractionSummary(models.Model):
    id = models.BigAutoField(primary_key=True)
    campaign = models.ForeignKey(
        Campaign,
        db_column="campaignId",
        on_delete=models.DO_NOTHING,
        related_name="domain_interaction_summaries",
    )
    domain = models.TextField()
    sent = models.IntegerField(default=0)
    open = models.IntegerField(default=0)
    click = models.IntegerField(default=0)
    soft_bounce = models.IntegerField(db_column="softBounce", default=0)
    hard_bounce = models.IntegerField(db_column="hardBounce", default=0)
    unique_open = models.IntegerField(db_column="uniqueOpen", default=0)
    unique_click = models.IntegerField(db_column="uniqueClick", default=0)
    created_at = models.DateTimeField(db_column="createdAt")
    updated_at = models.DateTimeField(db_column="updatedAt")

    class Meta:
        managed = True
        db_table = "DomainInteractionSummary"
        unique_together = (("campaign", "domain"),)
