from django.db import models


class User(models.Model):
    id = models.AutoField(primary_key=True)
    email = models.EmailField(unique=True)
    created_at = models.DateTimeField(db_column="createdAt")
    updated_at = models.DateTimeField(db_column="updatedAt", null=True, blank=True)
    password = models.TextField()

    class Meta:
        managed = True
        db_table = "User"

    def __str__(self):
        return self.email
