from django.db import models

# Create your models here.
class CompanyList(models.Model):
    company_id = models.AutoField(primary_key=True)
    company_trade_name = models.CharField(max_length=255)
    company_url = models.URLField()

    class Meta:
        db_table = 'company_list'
        managed = False  # prevent Django from creating/modifying this table