from django.contrib import admin, messages
from django.utils.html import format_html, format_html_join

from . import bus, services
from .models import Camera, Event, Person, RuntimeConfig, Snapshot, Tracklet


# ==================================================================== 인물
class SnapshotInline(admin.TabularInline):
    model = Snapshot
    extra = 0
    fields = ("preview", "tracklet", "score", "created_at")
    readonly_fields = ("preview", "created_at")

    def preview(self, obj):
        if obj.image:
            return format_html('<img src="{}" style="height:80px;border-radius:4px">',
                               obj.image.url)
        return "—"
    preview.short_description = "크롭"


class TrackletInline(admin.TabularInline):
    model = Tracklet
    extra = 0
    fields = ("camera", "local_id", "start_at", "end_at", "frames")
    readonly_fields = fields
    can_delete = False
    show_change_link = True


@admin.action(description="선택한 인물을 하나로 병합")
def action_merge(modeladmin, request, queryset):
    try:
        keep, n = services.merge_persons(queryset)
    except ValueError as e:
        modeladmin.message_user(request, str(e), messages.WARNING)
        return
    modeladmin.message_user(request, f"{n}명을 '{keep}'(으)로 병합했다", messages.SUCCESS)


@admin.action(description="검수 완료로 표시")
def action_confirm(modeladmin, request, queryset):
    n = queryset.update(confirmed=True)
    modeladmin.message_user(request, f"{n}명 검수 완료 처리", messages.SUCCESS)


@admin.action(description="선택한 인물 삭제 (크롭 파일 포함)")
def action_delete(modeladmin, request, queryset):
    n = services.delete_persons(list(queryset))
    modeladmin.message_user(request, f"{n}명 삭제", messages.SUCCESS)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ("id", "thumb", "label", "cam_badges", "track_count",
                    "confirmed", "last_seen")
    list_display_links = ("id", "thumb")
    list_editable = ("label", "confirmed")      # 목록에서 바로 이름 입력
    list_filter = ("confirmed", "is_active", "tracklets__camera")
    search_fields = ("label", "id")
    date_hierarchy = "created_at"
    inlines = [TrackletInline, SnapshotInline]
    actions = [action_merge, action_confirm, action_delete]
    list_per_page = 40

    def thumb(self, obj):
        s = obj.best_snapshot()
        if s and s.image:
            return format_html(
                '<img src="{}" style="height:64px;width:auto;border-radius:4px;'
                'background:#222">', s.image.url)
        return format_html('<span style="color:#999">없음</span>')
    thumb.short_description = "대표"

    def cam_badges(self, obj):
        """이 인물이 어느 카메라에 잡혔는지 — Re-ID 결과를 한눈에 보는 칸."""
        cams = obj.cameras_seen()
        if not cams:
            return "—"
        badges = format_html_join(
            " ", '<span style="background:#2b3b4d;color:#cfe3ff;padding:2px 7px;'
                 'border-radius:3px;font-size:11px;white-space:nowrap">{}</span>',
            ((c.name,) for c in cams))
        return format_html('<div style="white-space:nowrap">{}</div>', badges)
    cam_badges.short_description = "출현 카메라"

    def track_count(self, obj):
        return obj.tracklets.count()
    track_count.short_description = "트랙렛"

    def get_queryset(self, request):
        return (super().get_queryset(request)
                .prefetch_related("tracklets__camera", "snapshots"))


# ==================================================================== 트랙렛
@admin.action(description="선택한 트랙렛을 새 인물로 분리")
def action_split(modeladmin, request, queryset):
    try:
        new = services.split_tracklets(list(queryset))
    except ValueError as e:
        modeladmin.message_user(request, str(e), messages.WARNING)
        return
    modeladmin.message_user(request,
                            f"'{new}'(id={new.pk}) 로 분리했다", messages.SUCCESS)


@admin.register(Tracklet)
class TrackletAdmin(admin.ModelAdmin):
    list_display = ("id", "thumb", "person", "camera", "local_id",
                    "start_at", "duration_sec", "frames")
    list_filter = ("camera", "start_at")
    search_fields = ("person__label", "person__id", "local_id")
    autocomplete_fields = ("person",)
    actions = [action_split]

    def thumb(self, obj):
        s = obj.snapshots.order_by("-score").first()
        if s and s.image:
            return format_html('<img src="{}" style="height:56px;border-radius:4px">',
                               s.image.url)
        return "—"
    thumb.short_description = "크롭"


# ==================================================================== 기타
@admin.register(Camera)
class CameraAdmin(admin.ModelAdmin):
    list_display = ("index", "name", "source", "enabled", "note")
    list_editable = ("name", "source", "enabled", "note")

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        bus.send_command("reload_cameras")
        self.message_user(request, "카메라 설정을 tracker 에 전달했다", messages.INFO)


@admin.register(Snapshot)
class SnapshotAdmin(admin.ModelAdmin):
    list_display = ("id", "preview", "person", "tracklet", "score", "created_at")
    list_filter = ("tracklet__camera",)
    readonly_fields = ("preview",)

    def preview(self, obj):
        if obj.image:
            return format_html('<img src="{}" style="height:90px;border-radius:4px">',
                               obj.image.url)
        return "—"
    preview.short_description = "크롭"


@admin.register(Event)
class EventAdmin(admin.ModelAdmin):
    list_display = ("at", "person", "camera", "kind", "detail")
    list_filter = ("kind", "camera")
    date_hierarchy = "at"


@admin.register(RuntimeConfig)
class RuntimeConfigAdmin(admin.ModelAdmin):
    list_display = ("__str__", "det_conf", "reid_threshold", "max_gallery",
                    "updated_at")

    def has_add_permission(self, request):
        return not RuntimeConfig.objects.exists()   # 싱글톤

    def has_delete_permission(self, request, obj=None):
        return False

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        bus.send_command("set_config",
                         det_conf=obj.det_conf,
                         reid_threshold=obj.reid_threshold,
                         max_gallery=obj.max_gallery,
                         draw_boxes=obj.draw_boxes,
                         draw_labels=obj.draw_labels)
        self.message_user(request, "설정을 tracker 에 반영했다", messages.INFO)
