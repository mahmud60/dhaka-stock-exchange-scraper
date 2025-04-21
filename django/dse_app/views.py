from django.shortcuts import render
from .models import CompanyList

def show_companies(request):
    companies = CompanyList.objects.all()
    return render(request, 'dse/company_list.html', {'companies': companies})