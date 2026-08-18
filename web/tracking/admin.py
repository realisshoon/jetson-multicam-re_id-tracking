from django.contrib import admin, messages
from django.contrib.auth.admin import UserAdmin as DjangoUserAdmin
from django.contrib.auth.models import User
from django.utils.html import format_html, format_html_join

from . import bus, services
from .models import (Camera, Event, Journey, Person, RuntimeConfig, Snapshot,
                     Tracklet)


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
    list_display = ("index", "name", "jetson_host", "jetson_port",
                    "source", "enabled", "note")
    list_editable = ("name", "jetson_host", "jetson_port",
                     "source", "enabled", "note")

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


@admin.register(Journey)
class JourneyAdmin(admin.ModelAdmin):
    """메인 서버(B)가 신원의 source of truth 다 — 여긴 조회 전용.
    실제 MERGE_EXISTING/CONFIRM_NEW 액션은 나중에 REST API 로 붙일
    예정이라(2026-08-11 B 지시) 지금은 필드 전부 readonly."""
    list_display = ("journey_id", "display_person_uid_col", "final_review_result",
                    "journey_status", "person_status", "route", "entry_at",
                    "journey_elapsed_seconds", "final_score", "visit_count")
    list_filter = ("final_review_result", "journey_status", "person_status")
    search_fields = ("journey_id", "canonical_person_uid", "temporary_person_uid",
                     "candidate_person_uid", "final_candidate_person_uid")
    date_hierarchy = "entry_at"
    readonly_fields = [f.name for f in Journey._meta.fields]

    def display_person_uid_col(self, obj):
        return obj.display_person_uid() or "—"
    display_person_uid_col.short_description = "Person ID"

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


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


# ==================================================================== 사용자
# 기본 "Users" 메뉴를 "사용자관리"로 보이게, User 를 프록시로 다시 등록한다
# (실제 테이블/모델은 그대로 auth_user 다 — 그냥 admin 사이드바 표시 이름만
# 바꾸는 거라 프록시 모델이 정석). settings.py 의 INSTALLED_APPS 순서상
# auth 가 tracking 보다 먼저 로드되므로, 여기서 unregister(User) 할 때는
# 이미 auth.admin 이 등록해 둔 뒤라 정상적으로 바꿔치기된다.
class UserProxy(User):
    class Meta:
        proxy = True
        verbose_name = "사용자"
        verbose_name_plural = "사용자관리"


admin.site.unregister(User)


@admin.register(UserProxy)
class UserManagementAdmin(DjangoUserAdmin):
    """관리자가 '사용자 추가'로 계정을 만들면 그 즉시 로그인해서 대시보드를
    쓸 수 있어야 한다. 근데 Django 기본 "사용자 추가" 폼은 아이디/비밀번호만
    받고 '스태프 권한(is_staff)' 은 기본 꺼진 채로 만들어진다 — 로그인
    게이트가 /admin/login/ 인데 이건 is_staff 없으면 비밀번호가 맞아도
    로그인을 거부한다. 그래서 저장 시점에 새 계정이면 항상 is_staff=True
    로 만들어서, 만들자마자 바로 로그인되게 한다."""
    def save_model(self, request, obj, form, change):
        if not change:
            obj.is_staff = True
        super().save_model(request, obj, form, change)
