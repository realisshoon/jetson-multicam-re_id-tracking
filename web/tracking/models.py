import numpy as np
from django.conf import settings
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
    """Global ID. 카메라를 가로질러 동일인으로 묶인 단위.

    2026-08-11 부로 메인 서버(B)가 신원(person_uid)/여정(journey_id)의
    source of truth 다 — Camera A 는 신원을 스스로 정하지 않고, 메인
    서버가 Re-ID/DB 조회 후 person_uid 를 배정한다. journey_id 는 방문
    1회짜리 세션이라 사람 단위로 묶을 땐 person_uid 를 써야 한다
    (한 사람이 journey_id 여러 개를 가질 수 있음 = 반복 방문)."""
    label       = models.CharField("이름", max_length=50, blank=True)
    external_id = models.CharField(
        "외부 ID(person_uid)", max_length=64, unique=True, null=True, blank=True,
        db_index=True,
        help_text="메인 서버가 배정하는 person_uid (예: P000002). "
                  "메인 서버 API로 들어온 인물만 채워진다.")
    visit_count = models.IntegerField(
        "방문 횟수", default=1,
        help_text="메인 서버가 관리하는 값을 그대로 받아 저장 (journey_id 개수)")
    created_at = models.DateTimeField("최초 등장", default=timezone.now)
    last_seen  = models.DateTimeField("최근 등장", default=timezone.now)
    is_active  = models.BooleanField("활성", default=True)
    confirmed  = models.BooleanField("검수 완료", default=False,
                                     help_text="사람이 직접 확인한 인물 — 메인 서버 "
                                               "데이터가 아니라 이 대시보드에서만 관리하는 "
                                               "로컬 판단(등록/미등록 알림 기준)")

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

    def is_first_visit(self):
        """visit_count 는 메인 서버가 관리하는 값을 그대로 받아 쓴다
        (journey_id 개수) — 1 이하면 이번이 첫 방문."""
        return self.visit_count <= 1


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

    # 2026-08-12: "미등록자 감지 기록"에 사진을 같이 보여달라는 요청 —
    # 캡처 이미지(body_images/face_images)는 Journey 에 저장돼 있어서,
    # 이 감지가 어느 journey 소속인지 알아야 사진을 찾을 수 있다.
    # Journey 가 이 파일 뒤쪽에 정의돼 있어서 문자열로 참조한다.
    journey = models.ForeignKey(
        "Journey", on_delete=models.SET_NULL, null=True, blank=True,
        related_name="events", verbose_name="여정")

    # "미등록자 감지 기록" 패널은 이 값으로 필터링한다 — Person.confirmed
    # 를 그때그때 다시 보면 안 된다: "오류" 처리로 어떤 사람이 나중에
    # confirmed=True 가 되는 순간, 그 사람의 예전 감지 기록들까지 전부
    # 조용히 로그에서 사라져버린다(회색 처리가 아니라 통째로 안 보임).
    # 그래서 "이 이벤트가 찍힌 순간에 미등록자였는지"를 이 필드에 그대로
    # 못박아 둔다 — 나중에 그 사람이 확정되어도 과거 기록은 그대로 남고
    # review_status 로만 회색 처리한다.
    was_unregistered = models.BooleanField("찍힌 시점에 미등록자였음", default=True)

    # 2026-08-12: "미등록자 감지 기록"에서 관리자가 건별로 확인/오류 체크할
    # 수 있게 붙인 검토 상태 — 이건 메인 서버(B)의 Journey 신원 판정과는
    # 별개로, 이 대시보드에서만 관리하는 로컬 리뷰 워크플로다.
    # "확인" = 미등록자로 뜬 게 맞다고 검토 완료.
    # "오류" = 오탐(사실 알고 있는 사람)이라 리뷰 화면에서 실제 인물을
    #          찾아 연결한다 — 연결되면 그 Person.confirmed 를 True 로
    #          바꿔서, 그 사람의 다음 감지부터는 이 로그에 아예 안 뜬다
    #          (이 로그 자체가 confirmed=False 인 사람만 보여주는 필터라).
    REVIEW_CONFIRMED, REVIEW_ERROR = "confirmed", "error"
    REVIEW_STATUS = [(REVIEW_CONFIRMED, "확인"), (REVIEW_ERROR, "보류")]
    review_status = models.CharField("검토 상태", max_length=10,
                                     choices=REVIEW_STATUS, blank=True)
    reviewed_at   = models.DateTimeField("검토 시각", null=True, blank=True)
    reviewed_by   = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL,
                                      null=True, blank=True, verbose_name="검토자")
    resolved_person = models.ForeignKey(
        Person, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="resolved_events", verbose_name="실제 인물(오류 처리 시)",
        help_text="'오류' 처리할 때 관리자가 찾아 연결한 실제 인물")

    class Meta:
        verbose_name = verbose_name_plural = "이벤트"
        ordering = ["-at"]
        indexes = [models.Index(fields=["-at"])]

    def __str__(self):
        return f"[{self.at:%H:%M:%S}] {self.person} {self.get_kind_display()}"


class Journey(models.Model):
    """메인 서버(B)가 최종 처리하는 방문 1회짜리 세션. Re-ID/Journey/
    NEW·REVISIT 판정은 전부 메인 서버가 하고, Django 는 그 결과를 그대로
    받아 보여주기만 한다 — 이 모델 자체는 판단 로직을 갖지 않는다.

    2026-08-11 B 확정: 웹은 Jetson 의 로컬 트랙 ID(예: "D Local Track=13")
    로 사람을 관리하면 안 되고 person_uid/journey_id 기준으로 관리해야
    한다. 특히 temporary_person_uid 는 Final Review 확정 전 임시값이라
    화면에 "최종 사람 ID"로 보여주면 안 된다 — Final Review 완료 후엔
    canonical_person_uid 가 진짜 Person ID다(= Person.external_id)."""
    NEW, REVISIT, MANUAL_REVIEW = "NEW", "REVISIT", "MANUAL_REVIEW_REQUIRED"
    REVIEW_RESULT = [
        (NEW, "신규(NEW)"),
        (REVISIT, "재방문(REVISIT)"),
        (MANUAL_REVIEW, "검토 필요(MANUAL_REVIEW_REQUIRED)"),
    ]

    journey_id = models.CharField("Journey ID", max_length=32, unique=True,
                                  db_index=True, help_text="예: J000104")
    person = models.ForeignKey(
        Person, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="journeys", verbose_name="확정 인물",
        help_text="canonical_person_uid 가 확정된 후에만 연결된다. "
                  "MANUAL_REVIEW_REQUIRED 인 동안은 비어있다.")

    person_status = models.CharField(
        "인물 상태", max_length=30, blank=True,
        help_text="이 여정 기준 방문 성격 — 예: RETURNING (실API 필드: person_status)")
    journey_status = models.CharField(
        "여정 상태", max_length=30, blank=True,
        help_text="여정 자체의 진행 상태 — 예: COMPLETED/EXPIRED/ACTIVE "
                  "(person_status 와 별개 필드, 실API: journey_status)")
    route = models.CharField("경로", max_length=120, blank=True,
                             help_text='실API는 배열(["A","C","D"])로 오는 걸 '
                                       '"A -> C -> D" 문자열로 합쳐서 저장')
    entry_at = models.DateTimeField("진입 시각", null=True, blank=True)
    d_exit_at = models.DateTimeField("D 이탈 시각", null=True, blank=True)
    journey_elapsed_seconds = models.FloatField("소요 시간(초)", null=True, blank=True)
    visit_count = models.IntegerField("방문 횟수(당시 스냅샷)", null=True, blank=True)

    # Final Identity Review 결과 — 메인 서버가 신원을 어떻게 판단했는지의
    # 전체 과정. temporary/candidate 는 참고용이고, "이 사람이 누구다"라는
    # 결론은 canonical_person_uid + final_review_result 만 본다.
    # 2026-08-11 실API 확인(GET /api/journeys/{id}): 이 필드들은 응답의
    # "identity" 하위 객체에 들어있고, 필드명도 최초 안내와 다르다 —
    # candidate_person_uid → identity.initial_candidate_person_uid,
    # final_review_result → identity.final_result. main_api_ingest.py 의
    # ingest_journey() 가 이 매핑을 담당한다.
    initial_decision = models.CharField("초기 판정", max_length=40, blank=True)
    temporary_person_uid = models.CharField(
        "임시 UID", max_length=32, blank=True,
        help_text="화면에 최종 Person ID로 노출 금지 — Final Review 전 임시값")
    candidate_person_uid = models.CharField(
        "후보 UID", max_length=32, blank=True,
        help_text="실API: identity.initial_candidate_person_uid")
    final_candidate_person_uid = models.CharField("최종 후보 UID", max_length=32, blank=True)
    final_score = models.FloatField(
        "최종 스코어(단일)", null=True, blank=True,
        help_text="실API: identity.final_score — 예: 0.850")
    canonical_person_uid = models.CharField(
        "확정 UID", max_length=32, blank=True, db_index=True,
        help_text="Final Review 완료 후 실제 Person ID. Person.external_id 와 일치")
    final_review_result = models.CharField(
        "최종 판정", max_length=24, choices=REVIEW_RESULT, blank=True, db_index=True,
        help_text="실API: identity.final_result")
    final_scores = models.JSONField(
        "스코어 상세", null=True, blank=True,
        help_text="실API: identity.final_scores — body/face 모달리티별 세부 점수")

    # 2026-08-12: Camera A 캡처 이미지. Main 이 이제 상세 응답에
    # `capture_groups.A.body`/`.face` 로 [{rank, quality, url}, ...] 를
    # 내려준다(최대 3장씩) — url 은 Camera A Jetson 이 직접 서빙하는 LAN
    # 주소(예: http://10.10.20.56:8000/captures/...)라 브라우저에 그대로
    # 노출하지 않고 Django 인증 프록시(views.api_capture_proxy)를 거친다.
    # 이미지 바이트 자체는 저장하지 않는다 — Main/Camera A 가 원본이고
    # 우리는 url 목록만 캐시한다.
    body_images = models.JSONField("BODY 캡처", null=True, blank=True)
    face_images = models.JSONField("FACE 캡처", null=True, blank=True)

    # 2026-08-12: "검토 필요(MANUAL_REVIEW_REQUIRED)" 카드를 관리자가 클릭해서
    # 사진 후보 모달로 실제 인물을 지정할 수 있게 붙인 필드 — canonical_person_uid
    # 는 Main 만의 source of truth라 여기서 덮어쓰지 않는다(다음 폴링에서
    # Main 판정으로 그대로 되돌아옴). 이 필드들은 main_api_ingest.ingest_journey()
    # 의 update_or_create defaults 에 없어서 폴링이 지나가도 안 지워지는,
    # 순수 로컬 참고용 매칭이다.
    local_match_person = models.ForeignKey(
        Person, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="local_matched_journeys", verbose_name="관리자 로컬 매칭",
        help_text="관리자가 사진 보고 직접 지정한 추정 인물(Main 공식 판정과 별개)")
    local_match_at = models.DateTimeField("로컬 매칭 시각", null=True, blank=True)
    local_match_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="+", verbose_name="매칭한 관리자")

    # 2026-08-12: "보류 리스트"(구 미등록자 감지 기록) 개편 — MANUAL_REVIEW_
    # REQUIRED 여정(Main Re-ID 가 후보를 못 골라 "헷갈리는" 상태)을 이제
    # 여기서 직접 검토한다. Event.REVIEW_STATUS 와 같은 값을 쓴다 —
    # "confirmed" = 미등록자로 뜬 게 맞다고 확인만, "error"(화면 표기는
    # "보류") = local_match_person 으로 실제 인물을 지정. 이 값이 채워지면
    # 대시보드 실시간 위젯에서는 빠지고(cam_detections 필터) journeys.html
    # 의 "검토 필요" 표에서만 계속 보인다 — 처리했다고 기록 자체가 없어지는
    # 게 아니라 실시간 화면에서만 안 보이는 것.
    review_status = models.CharField("검토 상태", max_length=10,
                                     choices=Event.REVIEW_STATUS, blank=True)

    created_at = models.DateTimeField("최초 수신", auto_now_add=True)
    updated_at = models.DateTimeField("마지막 갱신", auto_now=True)

    class Meta:
        verbose_name = verbose_name_plural = "여정(Journey)"
        ordering = ["-entry_at", "-created_at"]

    def __str__(self):
        return self.journey_id

    def needs_review(self):
        return self.final_review_result == self.MANUAL_REVIEW

    def display_person_uid(self):
        """화면에 보여줄 최종 사람 ID. temporary_person_uid 는 최종 ID로
        절대 쓰지 않는다(B 지시사항) — 아직 미확정이면 빈 값을 돌려준다."""
        return self.canonical_person_uid

    def thumb_capture(self):
        """대시보드 카드용 대표 썸네일 1장 — FACE rank1 이 있으면 그걸,
        없으면 BODY rank1. (modality, rank) 튜플, 없으면 None."""
        for images, modality in ((self.face_images, "face"), (self.body_images, "body")):
            for im in (images or []):
                if im.get("rank") == 1:
                    return (modality, 1)
        return None


class AiMatchFeedback(models.Model):
    """"데이터 선택" 모달에서 관리자가 최종 확정한 결과와, Main 이 미리
    제안했던 후보(candidate_person_uid/final_candidate_person_uid)를 나란히
    기록한다 — 2026-08-13 요청: "AI가 잘못된 정보를 선택한 경우의 데이터도
    수집해줘. 학습 데이터를 Main 에게 전달은 아직 안 하고 로컬에 수집만."
    그래서 Main 에 보내는 코드는 없고, 여기 로컬 테이블에만 쌓인다."""
    journey = models.ForeignKey(
        "Journey", on_delete=models.SET_NULL, null=True, blank=True,
        related_name="ai_feedback", verbose_name="여정")
    event = models.ForeignKey(
        "Event", on_delete=models.SET_NULL, null=True, blank=True,
        related_name="ai_feedback", verbose_name="이벤트")
    suggested_person_uid = models.CharField(
        "AI(Main) 제안 후보", max_length=32, blank=True)
    confirmed_person_uid = models.CharField(
        "관리자 최종 확정", max_length=32, blank=True,
        help_text="비어있으면 '특정 인물 없음(미등록자 확인)'으로 확정한 것")
    was_correct = models.BooleanField("AI 제안이 맞았는지")
    reviewed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True,
        verbose_name="검토한 관리자")
    created_at = models.DateTimeField("기록 시각", default=timezone.now)

    class Meta:
        verbose_name = verbose_name_plural = "AI 매칭 피드백"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.suggested_person_uid or '—'} → {self.confirmed_person_uid or '—'} ({'O' if self.was_correct else 'X'})"


class RuntimeConfig(models.Model):
    """(C) 파이프라인 파라미터. 싱글톤으로 쓴다."""
    det_conf       = models.FloatField("검출 conf", default=0.40)
    reid_threshold = models.FloatField("Re-ID 유사도 컷", default=0.55)
    max_gallery    = models.IntegerField("인물당 갤러리 최대", default=5)
    draw_boxes     = models.BooleanField("박스 표시", default=True)
    draw_labels    = models.BooleanField("ID 표시", default=True)

    # MQTT 브로커 직접 연결(구 방식) — B(메인 서버)가 REST API를 제공하기로
    # 확정되면서(2026-08-11) 더 이상 쓰지 않는다. mqtt_worker.py 는 이제
    # 실행하지 않고 main_server_worker.py 로 대체했다. 롤백 대비로 필드/
    # 코드는 남겨둔다.
    jetson_host       = models.CharField("MQTT 브로커 IP (구, 미사용)", max_length=100,
                                         default="10.10.20.56")
    jetson_mqtt_port  = models.IntegerField("MQTT 포트 (구, 미사용)", default=1883)

    # 메인 서버(B) REST API 접속 정보 — 확정된 구조: Jetson A/B/C/D → MQTT →
    # 메인 서버(Windows) → main_server.db → REST API → 이 Django. 주소가
    # 바뀌면 여기 값만 바꾸면 되고, main_server_worker.py 가 주기적으로
    # 확인해서 반영한다(감지 IP 설정과 같은 패턴).
    main_server_host = models.CharField("메인 서버 IP", max_length=100,
                                        default="10.10.20.33")
    main_server_port = models.IntegerField("메인 서버 API 포트", default=8080)

    # Jetson 장비 자체(카메라 캡처·YOLO·Re-ID)는 원격으로 켜고 끌 방법이
    # 없다 — 그쪽은 항상 자체적으로 돈다. 대신 "감지 on/off"는 우리 쪽에서
    # 메인 서버 API 폴링을 멈추는 걸로 흉내낸다: 꺼두면 main_server_worker.py
    # 가 새 감지 이벤트를 아예 안 받아온다(알림/소리/기록 전부 멈춘다).
    # 영상은 브라우저가 Jetson 에 직접 붙는 거라 이 스위치와 무관하게
    # 계속 나온다.
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
