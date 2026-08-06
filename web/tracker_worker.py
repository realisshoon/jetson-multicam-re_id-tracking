#!/usr/bin/env python
"""
추론 파이프라인 프로세스. Django 와 **별도 프로세스**로 돈다.

이걸 Django 안에서 돌리면 안 되는 이유:
  - 워커가 fork 될 때마다 모델이 GPU 에 중복 로드된다 (Orin Nano 8GB 공유 → OOM)
  - autoreload 가 코드 저장할 때마다 파이프라인을 죽인다
  - 요청 처리와 추론이 GIL 을 두고 싸운다

Django ORM 은 django.setup() 만 부르면 그대로 쓸 수 있다.

jetson-multicam-re_id-tracking 연동(MQTT cctv/entry 구독)은 이 파일이 아니라
별도 프로세스인 mqtt_worker.py 가 담당한다. 이 파일은 로컬에 붙은 카메라를
Django 대시보드의 시작/정지 버튼으로 직접 제어하는 용도다.

실행:  python tracker_worker.py
"""
import os
import sys
import time
from datetime import timedelta

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()                                    # ← ORM 쓰려면 반드시 먼저

import cv2                                        # noqa: E402
import numpy as np                                # noqa: E402
from django.core.files.base import ContentFile    # noqa: E402
from django.utils import timezone                 # noqa: E402

from tracking import bus                          # noqa: E402
from tracking.models import (Camera, Event, Person,  # noqa: E402
                             RuntimeConfig, Snapshot, Tracklet)

# ── 기존 레포 코드를 여기에 연결한다 ────────────────────────────────
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
# from src.detector import YoloDetector
# from src.tracker import ByteTracker
# from src.reid import OsnetExtractor
# --------------------------------------------------------------------

TRACKLET_GAP = timedelta(seconds=3)     # 이 시간 넘게 안 보이면 트랙렛 종료


class Pipeline:
    def __init__(self):
        self.running = False
        self.caps = {}                  # {cam_index: VideoCapture}
        self.cams = {}                  # {cam_index: Camera}
        self.active = {}                # {(cam_index, local_id): Tracklet}
        self.gallery = {}               # {person_id: np.ndarray centroid}
        self.cfg = RuntimeConfig.get()
        self.fps = 0.0

        # self.det  = YoloDetector(conf=self.cfg.det_conf)
        # self.trk  = {}   # 카메라별 ByteTrack 인스턴스
        # self.reid = OsnetExtractor()

        self.reload_cameras()
        self.reload_gallery()

    # ── 카메라 (C) ---------------------------------------------------
    def reload_cameras(self):
        for c in self.caps.values():
            c.release()
        self.caps, self.cams = {}, {}
        for cam in Camera.objects.filter(enabled=True):
            src = int(cam.source) if cam.source.isdigit() else cam.source
            cap = cv2.VideoCapture(src)
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)     # 지연 누적 방지
            if cap.isOpened():
                self.caps[cam.index] = cap
                self.cams[cam.index] = cam
                print(f"[cam] {cam} 열림")
            else:
                print(f"[cam] {cam} 열기 실패: {cam.source}", file=sys.stderr)

    # ── 갤러리 (B 와 연동) -------------------------------------------
    def reload_gallery(self, person_id=None):
        """Admin 에서 병합/분리하면 여기가 다시 불린다.
        이걸 안 하면 병합해도 다음 프레임에 도로 갈라진다."""
        qs = (Person.objects.filter(pk=person_id) if person_id
              else Person.objects.filter(is_active=True))
        for p in qs.prefetch_related("snapshots"):
            c = p.centroid()
            if c is not None:
                self.gallery[p.pk] = c
            else:
                self.gallery.pop(p.pk, None)
        if person_id and not Person.objects.filter(pk=person_id).exists():
            self.gallery.pop(person_id, None)
        print(f"[reid] 갤러리 {len(self.gallery)}명 로드")

    def match(self, vec):
        """코사인 유사도 최대값이 컷을 넘으면 그 person_id, 아니면 None."""
        if not self.gallery:
            return None, 0.0
        ids = list(self.gallery)
        mat = np.stack([self.gallery[i] for i in ids])
        sims = mat @ (vec / (np.linalg.norm(vec) + 1e-9))
        k = int(np.argmax(sims))
        return (ids[k], float(sims[k])) if sims[k] >= self.cfg.reid_threshold \
            else (None, float(sims[k]))

    # ── 명령 (C) -----------------------------------------------------
    def handle_commands(self):
        while True:
            msg = bus.pop_command()      # 논블로킹
            if not msg:
                return
            cmd, args = msg["cmd"], msg.get("args", {})
            print(f"[cmd] {cmd} {args}")
            if cmd == "start":
                self.running = True
            elif cmd == "stop":
                self.running = False
            elif cmd == "reload_cameras":
                self.reload_cameras()
            elif cmd == "reload_gallery":
                pid = args.get("person_id")
                self.reload_gallery(int(pid) if pid else None)
            elif cmd == "set_config":
                self.cfg.refresh_from_db()

    # ── 트랙렛 / DB ---------------------------------------------------
    def touch_tracklet(self, cam, local_id, person, now):
        key = (cam.index, local_id)
        tl = self.active.get(key)
        if tl and (now - (tl.end_at or tl.start_at)) < TRACKLET_GAP:
            tl.end_at, tl.frames = now, tl.frames + 1
            tl.save(update_fields=["end_at", "frames"])
            return tl
        tl = Tracklet.objects.create(person=person, camera=cam,
                                     local_id=local_id, start_at=now,
                                     end_at=now, frames=1)
        self.active[key] = tl
        Event.objects.create(person=person, camera=cam,
                             kind=Event.ENTER, at=now)   # 전이에만 기록
        return tl

    def save_snapshot(self, person, tracklet, crop_bgr, vec, score):
        if person.snapshots.count() >= self.cfg.max_gallery:
            worst = person.snapshots.order_by("score").first()
            if worst and worst.score >= score:
                return
            worst.image.delete(save=False)
            worst.delete()
        ok, buf = cv2.imencode(".jpg", crop_bgr, [cv2.IMWRITE_JPEG_QUALITY, 88])
        if not ok:
            return
        s = Snapshot(person=person, tracklet=tracklet, score=float(score))
        s.set_vector(vec)
        s.image.save(f"p{person.pk}_{int(time.time()*1000)}.jpg",
                     ContentFile(buf.tobytes()), save=True)

    # ── 메인 루프 -----------------------------------------------------
    def run(self):
        t_prev, n = time.time(), 0
        while True:
            self.handle_commands()
            if not self.running or not self.caps:
                bus.publish_state({"running": False, "fps": 0.0, "tracks": []})
                time.sleep(0.2)
                continue

            now = timezone.now()
            live = []

            for idx, cap in self.caps.items():
                ok, frame = cap.read()
                if not ok:
                    continue
                cam = self.cams[idx]

                # ── 여기에 기존 파이프라인을 꽂는다 ──────────────────
                # dets   = self.det(frame)                      # xyxy, conf
                # tracks = self.trk[idx].update(dets)           # + local_id
                tracks = []                                     # 임시
                # ────────────────────────────────────────────────────

                for t in tracks:
                    x1, y1, x2, y2 = map(int, t["bbox"])
                    crop = frame[max(y1,0):y2, max(x1,0):x2]
                    if crop.size == 0:
                        continue
                    # vec = self.reid(crop)
                    vec = np.zeros(512, dtype=np.float32)       # 임시
                    pid, sim = self.match(vec)

                    if pid is None:
                        person = Person.objects.create(created_at=now,
                                                       last_seen=now)
                        self.gallery[person.pk] = vec / (np.linalg.norm(vec)+1e-9)
                    else:
                        person = Person.objects.get(pk=pid)
                        person.last_seen = now
                        person.save(update_fields=["last_seen"])

                    tl = self.touch_tracklet(cam, t["local_id"], person, now)
                    if tl.frames % 15 == 1:        # 매 프레임 저장하면 DB 가 못 버틴다
                        self.save_snapshot(person, tl, crop, vec, t["conf"])

                    if self.cfg.draw_boxes:
                        cv2.rectangle(frame, (x1,y1), (x2,y2), (84,180,255), 2)
                    if self.cfg.draw_labels:
                        cv2.putText(frame, str(person), (x1, max(y1-7, 14)),
                                    cv2.FONT_HERSHEY_SIMPLEX, 0.6,
                                    (84,180,255), 2)
                    live.append({"person": person.pk, "cam": idx,
                                 "sim": round(sim, 3)})

                # 스트림용 리사이즈 + 인코딩
                h, w = frame.shape[:2]
                tw = 640
                if w > tw:
                    frame = cv2.resize(frame, (tw, int(h * tw / w)))
                ok, jpg = cv2.imencode(".jpg", frame,
                                       [cv2.IMWRITE_JPEG_QUALITY, 70])
                if ok:
                    bus.publish_frame(idx, jpg.tobytes())

            # FPS
            n += 1
            if time.time() - t_prev >= 1.0:
                self.fps = n / (time.time() - t_prev)
                t_prev, n = time.time(), 0

            bus.publish_state({"running": True, "fps": self.fps,
                               "tracks": live})


if __name__ == "__main__":
    print("[tracker] 시작. 대시보드에서 '시작' 을 누르면 추론이 돈다.")
    Pipeline().run()
