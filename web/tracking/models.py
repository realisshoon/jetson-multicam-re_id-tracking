import numpy as np
from django.db import models
from django.utils import timezone


class Camera(models.Model):
    """(C) 웹에서 카메라를 추가/편집하면 tracker 가 reload_cameras 로 반영한다."""
    name    = models.CharField("이름", max_length=50)
    source  = models.CharField(
        "소스", max_length=300,
        help_text="/dev/video0 또는 rtsp://... 또는 파일 경로")
    index   = models.IntegerField("스트림 번호", unique=True,
                                  help_text="/video/<번호> URL 에 쓰임")
    enabled = models.BooleanField("사용", default=True)
    note    = models.CharField("메모", max_length=200, blank=True)

    class Meta:
        verbose_name = verbose_name_plural = "카메라"
        ordering = ["index"]

    def __str__(self):
        return f"{self.index}. {self.name}"


class Person(models.Model):
    """Global ID. 카메라를 가로질러 동일인으로 묶인 단위."""
    label       = models.CharField("이름", max_length=50, blank=True)
    external_id = models.CharField(
        "외부 ID", max_length=64, unique=True, null=True, blank=True,
        db_index=True,
        help_text="jetson-multicam-re_id-tracking 의 global_person_id "
                  "(예: G000001). MQTT 로 들어온 인물만 채워진다.")
    created_at = models.DateTimeField("최초 등장", default=timezone.now)
    last_seen  = models.DateTimeField("최근 등장", default=timezone.now)
    is_active  = models.BooleanField("활성", default=True)
    confirmed  = models.BooleanField("검수 완료", default=False,
                                     help_text="사람이 직접 확인한 인물")

    class Meta:
        verbose_name = verbose_name_plural = "인물"
        ordering = ["-last_seen"]

    def __str__(self):
        return self.label or f"미확인 #{self.pk}"

    # --- Re-ID 갤러리 -------------------------------------------------
    def centroid(self):
        """이 인물의 대표 임베딩. 병합 후 재계산해서 tracker 에 넘긴다."""
        vecs = [s.vector() for s in self.snapshots.all() if s.embedding]
        if not vecs:
            return None
        m = np.mean(np.stack(vecs), axis=0)
        n = np.linalg.norm(m)
        return (m / n) if n else m

    def cameras_seen(self):
        return (Camera.objects
                .filter(tracklet__person=self)
                .distinct().order_by("index"))

    def best_snapshot(self):
        return self.snapshots.order_by("-score").first()


class Tracklet(models.Model):
    """카메라 1대에서 끊기지 않고 이어진 하나의 궤적.
    병합/분리는 이 단위로 한다 — Person 단위면 너무 굵고 Snapshot 단위면 너무 잘다."""
    person   = models.ForeignKey(Person, on_delete=models.CASCADE,
                                 related_name="tracklets", verbose_name="인물")
    camera   = models.ForeignKey(Camera, on_delete=models.CASCADE,
                                 verbose_name="카메라")
    local_id = models.IntegerField("로컬 ID", help_text="ByteTrack 이 부여한 번호")
    start_at = models.DateTimeField("시작", default=timezone.now)
    end_at   = models.DateTimeField("종료", null=True, blank=True)
    frames   = models.IntegerField("프레임 수", default=0)

    class Meta:
        verbose_name = verbose_name_plural = "트랙렛"
        ordering = ["-start_at"]
        indexes = [models.Index(fields=["camera", "local_id", "start_at"])]

    def __str__(self):
        return f"{self.camera.name} · local {self.local_id}"

    def duration_sec(self):
        return int(((self.end_at or timezone.now()) - self.start_at).total_seconds())


class Snapshot(models.Model):
    """갤러리용 크롭 이미지 + Re-ID 임베딩."""
    person     = models.ForeignKey(Person, on_delete=models.CASCADE,
                                   related_name="snapshots", verbose_name="인물")
    tracklet   = models.ForeignKey(Tracklet, on_delete=models.CASCADE, null=True,
                                   related_name="snapshots", verbose_name="트랙렛")
    image      = models.ImageField("크롭", upload_to="crops/%Y%m%d/", blank=True,
                                   help_text="MQTT 로 들어온 인물은 embedding 만 있고 "
                                             "크롭 이미지는 없을 수 있다.")
    embedding  = models.BinaryField("임베딩", null=True, blank=True)
    score      = models.FloatField("품질", default=0.0,
                                   help_text="검출 confidence. 대표 이미지 선정 기준")
    created_at = models.DateTimeField("시각", default=timezone.now)

    class Meta:
        verbose_name = verbose_name_plural = "스냅샷"
        ordering = ["-score"]

    def __str__(self):
        return f"snap#{self.pk} ({self.score:.2f})"

    def vector(self):
        return np.frombuffer(self.embedding, dtype=np.float32)

    def set_vector(self, arr):
        self.embedding = np.asarray(arr, dtype=np.float32).tobytes()


class Event(models.Model):
    """타임라인. 매 프레임이 아니라 상태 전이에만 기록한다."""
    ENTER, EXIT, MERGE, SPLIT = "enter", "exit", "merge", "split"
    KIND = [(ENTER, "진입"), (EXIT, "이탈"), (MERGE, "병합"), (SPLIT, "분리")]

    person = models.ForeignKey(Person, on_delete=models.CASCADE, verbose_name="인물")
    camera = models.ForeignKey(Camera, on_delete=models.SET_NULL, null=True,
                               blank=True, verbose_name="카메라")
    kind   = models.CharField("종류", max_length=10, choices=KIND)
    at     = models.DateTimeField("시각", default=timezone.now)
    detail = models.CharField("비고", max_length=200, blank=True)

    class Meta:
        verbose_name = verbose_name_plural = "이벤트"
        ordering = ["-at"]
        indexes = [models.Index(fields=["-at"])]

    def __str__(self):
        return f"[{self.at:%H:%M:%S}] {self.person} {self.get_kind_display()}"


class RuntimeConfig(models.Model):
    """(C) 파이프라인 파라미터. 싱글톤으로 쓴다."""
    det_conf       = models.FloatField("검출 conf", default=0.40)
    reid_threshold = models.FloatField("Re-ID 유사도 컷", default=0.55)
    max_gallery    = models.IntegerField("인물당 갤러리 최대", default=5)
    draw_boxes     = models.BooleanField("박스 표시", default=True)
    draw_labels    = models.BooleanField("ID 표시", default=True)
    updated_at     = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = verbose_name_plural = "런타임 설정"

    def __str__(self):
        return "런타임 설정"

    @classmethod
    def get(cls):
        obj, _ = cls.objects.get_or_create(pk=1)
        return obj
