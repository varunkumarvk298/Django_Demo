from django.db import models

# Create your models here.

class Book(models.Model):
    id=models.IntegerField(primary_key=True)
    title=models.CharField(max_length=200)
    authour=models.CharField(max_length=200)

class Student(models.Model):
    id=models.IntegerField(primary_key=True)
    first_name=models.CharField(max_length=20)
    last_name=models.CharField(max_length=20, null=True, blank=True)
    dob=models.DateField(null=True, blank=True)