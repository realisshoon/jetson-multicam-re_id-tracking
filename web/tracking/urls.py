from django.urls import path
from . import views

urlpatterns = [
    path("",                       views.dashboard,   name="dashboard"),
    path("video/<int:cam_index>/", views.mjpeg,       name="mjpeg"),
    path("api/state/",             views.api_state,   name="api_state"),
    path("api/stats/",             views.api_stats,   name="api_stats"),
    path("api/central/",           views.api_central, name="api_central"),
    path("api/control/",           views.api_control, name="api_control"),
    path("api/toggle_detection/",  views.api_toggle_detection,
                                   name="api_toggle_detection"),
    path("api/person/<int:person_id>/rename/",
                                   views.api_rename,  name="api_rename"),
    path("healthz/",               views.healthz,     name="healthz"),
]
