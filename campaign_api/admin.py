from django.contrib import admin

from .models import (
    Account,
    Campaign,
    Cluster,
    ClusterInteractionSummary,
    DomainInteractionSummary,
    UserInteraction,
)

admin.site.register(Account)
admin.site.register(Campaign)
admin.site.register(Cluster)
admin.site.register(UserInteraction)
admin.site.register(ClusterInteractionSummary)
admin.site.register(DomainInteractionSummary)
