from django.shortcuts import render
from django.http import HttpResponse
from rest_framework.views import APIView
from rest_framework.response import Response
from .models import *
from .serializers import *
from rest_framework import viewsets
# Create your views here.


class BookApiVIew(APIView):

    serializer_class=BookSerializer

    def get(self, request):
        allBooks = Book.objects.all().values()
        return Response({"Message":"List of Books","Book List":allBooks})

    def post(self, request):
        print("resquest data is : ", request.data)
        serializers_obj = BookSerializer(data=request.data)

        if(serializers_obj.is_valid()):

            Book.objects.create(id=serializers_obj.data.get("id"),
                            title=serializers_obj.data.get("title"),
                            authour=serializers_obj.data.get("authour")
                            )
        book = Book.objects.all().filter(id=request.data["id"]).values()
        return Response({"Message":"New Book","Book":book})



class StudentViewSet(viewsets.ModelViewSet):
    queryset=Student.objects.all()
    serializer_class =StudentSerializer