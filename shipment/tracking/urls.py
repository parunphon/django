from django.urls import path
from django.conf.urls import url
from django.contrib.auth import views as auth_views#
from . import views

urlpatterns = [
    path('', views.Home, name='home-page'),
    path('bot/', views.BOT, name='bot-page'),
    path('sql/', views.read_sql, name='sql-page'),
    path('test/', views.Test, name='test'),
    path('botlog/', views.Botlog, name='botlog-page'),
    path('another/', views.Another, name='another-page'),
    path('download/<process>', views.Download, name='another-page'),
    path('contact/', views.Contact, name='contact-page'),
    url(r'^$', views.Home),#
    url(r'^login/$', auth_views.LoginView.as_view()),#
    url(r'^logout/$', auth_views.LogoutView.as_view()),#

]
