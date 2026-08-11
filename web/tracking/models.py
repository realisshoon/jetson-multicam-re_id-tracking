import numpy as np
from django.db import models
from django.utils import timezone


class Camera(models.Model):
    """(C) 웹에서 카메라를 추가/편집하면 tracker 가 reload_cameras 로 반영한다.

    Jetson 보드가 여러 대(팀 계획상 총 4대, 각자 다른 IP)라 카메라마다
    자기 IP를 따로 들고 있는다 — jetson_host 를 채우면 대시보드가 그
    주소의 MJPEG 스트림을 바로 embed 하고(Django 를 거치지 않는다),
    비워두면 이 PC에 연결된 로컬 소스(source 필드)로 보고 내부 프록시를
    쓴다. mqtt_ingest.py 가 MQTT 로 처음 들어온 노드를 자동으로 카메라
    행으로 만들어주기도 하는데, 그때는 jetson_host 가 비어있으니 이
    관리자 화면에서 채워 넣어야 영상이 뜬다."""
    name    = models.CharField("이름", max_length=50)
    source  = models.CharField(
        "소스", max_length=300, blank=True,
        help_text="로컬 카메라만 씀: /dev/video0 또는 rtsp://... 또는 파일 경로")
    index   = models.IntegerField("스트림 번호", unique=True,
                                  help_text="/video/<번호> URL 에 쓰임")
    enabled = models.BooleanField("사용", default=True)
    note    = models.CharField("메모", max_length=200, blank=True)

    jetson_host = models.CharField(
        "Jetson IP", max_length=100, blank=True,
        help_text="이 카메라가 연결된 Jetson 보드 주소. 비워두면 로컬 카메라로 취급.")
    jetson_port = models.IntegerField(
        "Jetson 포트", null=True, blank=True, default=8000,
        help_text="그 보드에서 이 카메라 영상을 서빙하는 포트 (예: 8000)")

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

    # MQTT 브로커 접속 정보 — 예전엔 config/settings.py 에 박혀 있어서(환경변수로만
    # 오버라이드 가능) 주소가 바뀌면 서버를 껐다 켜야 했다. 이제 여기 저장해서
    # Django admin 에서 바로 바꿀 수 있고, mqtt_worker.py 가 주기적으로 이 값을
    # 확인해서 바뀌면 알아서 재연결한다. (카메라별 영상 주소는 이제 여기가 아니라
    # Camera.jetson_host/jetson_port 에 — 보드가 여러 대라 카메라마다 다르다.)
    jetson_host       = models.CharField("MQTT 브로커 IP", max_length=100,
                                         default="10.10.20.56")
    jetson_mqtt_port  = models.IntegerField("MQTT 포트", default=1883)

    # Jetson 장비 자체(카메라 캡처·YOLO·Re-ID)는 원격으로 켜고 끌 방법이
    # 없다 — 그쪽은 항상 자체적으로 돈다. 대신 "감지 on/off"는 우리 쪽
    # MQTT 수신을 끊는 걸로 흉내낸다: 꺼두면 mqtt_worker.py 가 브로커
    # 연결을 끊어서 새 감지 이벤트를 아예 받지도 처리하지도 않는다(알림/
    # 소리/기록 전부 멈춘다). 영상은 브라우저가 Jetson 에 직접 붙는
    # 거라 이 스위치와 무관하게 계속 나온다.
    detection_enabled = models.BooleanField("감지 활성화", default=True)

    updated_at     = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = verbose_name_plural = "런타임 설정"

    def __str__(self):
        return "런타임 설정"

    @classmethod
    def get(cls):
        obj, _ = cls.objects.get_or_create(pk=1)
        return obj
