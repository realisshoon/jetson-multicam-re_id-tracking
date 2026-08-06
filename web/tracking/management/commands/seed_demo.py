"""
파이프라인 없이 (B) 관리 화면부터 만들 수 있게 더미 데이터를 넣는다.
일정이 빡빡하면 이걸로 Admin 을 먼저 완성해두고, 라이브 연동은 나중에 붙이면 된다.

    python manage.py seed_demo --people 12 --reset
"""
import io
import random
from datetime import timedelta

import numpy as np
from django.core.files.base import ContentFile
from django.core.management.base import BaseCommand
from django.utils import timezone
from PIL import Image, ImageDraw

from tracking.models import Camera, Event, Person, Snapshot, Tracklet


def fake_crop(seed: int) -> bytes:
    """사람 실루엣처럼 보이는 64x128 더미 이미지."""
    rnd = random.Random(seed)
    hue = rnd.randint(0, 255)
    img = Image.new("RGB", (64, 128), (18, 24, 32))
    d = ImageDraw.Draw(img)
    top = ((hue * 7) % 200 + 40, (hue * 3) % 200 + 40, (hue * 11) % 200 + 40)
    bot = ((hue * 5) % 160 + 30, (hue * 13) % 160 + 30, (hue * 2) % 160 + 30)
    d.ellipse([24, 8, 40, 26], fill=(210, 180, 150))     # 머리
    d.rounded_rectangle([18, 28, 46, 76], 6, fill=top)   # 상의
    d.rounded_rectangle([22, 76, 42, 118], 4, fill=bot)  # 하의
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=85)
    return buf.getvalue()


class Command(BaseCommand):
    help = "데모용 인물/트랙렛/스냅샷 생성"

    def add_arguments(self, p):
        p.add_argument("--people", type=int, default=10)
        p.add_argument("--reset", action="store_true", help="기존 데이터 삭제")

    def handle(self, *a, **o):
        if o["reset"]:
            for s in Snapshot.objects.all():
                s.image.delete(save=False)
            Snapshot.objects.all().delete()
            Event.objects.all().delete()
            Tracklet.objects.all().delete()
            Person.objects.all().delete()
            self.stdout.write("기존 데이터 삭제")

        cams = []
        for i, name in enumerate(["입구", "복도", "출구"]):
            c, _ = Camera.objects.get_or_create(
                index=i, defaults={"name": name, "source": f"/dev/video{i}"})
            cams.append(c)

        now = timezone.now()
        rnd = random.Random(42)
        made = 0

        for k in range(o["people"]):
            t0 = now - timedelta(minutes=rnd.randint(1, 90))
            p = Person.objects.create(created_at=t0, last_seen=t0)

            # 3명 중 1명꼴로 카메라를 가로질러 매칭된 것처럼 만든다
            picks = rnd.sample(cams, 2 if k % 3 == 0 else 1)
            base = np.random.rand(512).astype(np.float32)
            base /= np.linalg.norm(base)

            for j, cam in enumerate(picks):
                start = t0 + timedelta(seconds=j * rnd.randint(8, 40))
                tl = Tracklet.objects.create(
                    person=p, camera=cam, local_id=rnd.randint(1, 60),
                    start_at=start,
                    end_at=start + timedelta(seconds=rnd.randint(4, 30)),
                    frames=rnd.randint(20, 400))
                Event.objects.create(person=p, camera=cam,
                                     kind=Event.ENTER, at=start)

                for n in range(rnd.randint(2, 4)):
                    s = Snapshot(person=p, tracklet=tl,
                                 score=round(rnd.uniform(0.45, 0.97), 3),
                                 created_at=start + timedelta(seconds=n))
                    noise = np.random.randn(512).astype(np.float32) * 0.05
                    v = base + noise
                    s.set_vector(v / np.linalg.norm(v))
                    s.image.save(f"demo_{p.pk}_{cam.index}_{n}.jpg",
                                 ContentFile(fake_crop(p.pk * 10 + n)), save=True)

                p.last_seen = tl.end_at
            p.save(update_fields=["last_seen"])
            made += 1

        self.stdout.write(self.style.SUCCESS(
            f"인물 {made}명 / 카메라 {len(cams)}대 생성 완료. "
            f"/admin/tracking/person/ 에서 확인하면 된다."))
