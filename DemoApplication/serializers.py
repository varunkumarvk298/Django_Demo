from rest_framework import serializers
from .models import *

class BookSerializer(serializers.Serializer):
    id=serializers.IntegerField(label="Enter Book ID")
    title=serializers.CharField(label="Enter Book Tilte")
    authour=serializers.CharField(label="Enter authour name")

class StudentSerializer(serializers.ModelSerializer):
    class Meta:
        model=Student
        fields="__all__"
    # id=serializers.IntegerField(label="ID")
    # first_name=serializers.CharField(label="Enter first_name")
    # last_name=serializers.CharField(label="Enter last_name")
    # dob=serializers.DateField(label="Enter dob")

