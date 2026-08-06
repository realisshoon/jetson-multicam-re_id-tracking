from django.urls import path
from . import views

urlpatterns = [
    path("",                       views.dashboard,   name="dashboard"),
    path("video/<int:cam_index>/", views.mjpeg,       name="mjpeg"),
    path("api/state/",             views.api_state,   name="api_state"),
    path("api/control/",           views.api_control, name="api_control"),
    path("api/person/<int:person_id>/rename/",
                                   views.api_rename,  name="api_rename"),
    path("healthz/",               views.healthz,     name="healthz"),
]
