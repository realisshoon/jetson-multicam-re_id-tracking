from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import include, path

admin.site.site_header = "Multicam Re-ID 관제"
admin.site.site_title  = "Re-ID Admin"
admin.site.index_title = "인물 · 카메라 관리"

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", include("tracking.urls")),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
