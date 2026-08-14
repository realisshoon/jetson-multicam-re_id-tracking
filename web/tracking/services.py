"""(B) 인물 데이터 관리 로직. Admin 액션과 API 가 같이 쓴다."""
from django.db import transaction

from . import bus
from .models import Event, Journey, Person, Snapshot, Tracklet


@transaction.atomic
def merge_persons(persons):
    """여러 인물을 하나로 합친다. 가장 오래된(=pk 작은) 인물로 흡수.

    반환: (남긴 Person, 흡수된 수)
    """
    persons = list(persons.order_by("pk") if hasattr(persons, "order_by")
                   else sorted(persons, key=lambda p: p.pk))
    if len(persons) < 2:
        raise ValueError("2명 이상 선택해야 병합할 수 있다")

    keep, drop = persons[0], persons[1:]
    drop_ids = [p.pk for p in drop]
    names = ", ".join(str(p) for p in drop)

    Tracklet.objects.filter(person_id__in=drop_ids).update(person=keep)
    Snapshot.objects.filter(person_id__in=drop_ids).update(person=keep)
    Event.objects.filter(person_id__in=drop_ids).update(person=keep)
    # 2026-08-12: 인물 상세 페이지 "수정 → 병합" 요청으로 붙임 — 흡수될
    # 인물이 Journey.person/local_match_person 으로도 걸려 있을 수 있고,
    # 다른 사람의 Event.resolved_person(§"보류" 처리로 지정된 실제 인물)
    # 으로 가리켜졌을 수도 있다. SET_NULL 이라 그냥 두면 delete() 때
    # 자동으로 null 처리되는데, 그러면 "누구인지" 정보가 사라지니 keep 으로
    # 옮겨서 병합 후에도 그대로 남게 한다.
    Journey.objects.filter(person_id__in=drop_ids).update(person=keep)
    Journey.objects.filter(local_match_person_id__in=drop_ids).update(local_match_person=keep)
    Event.objects.filter(resolved_person_id__in=drop_ids).update(resolved_person=keep)

    last = max((p.last_seen for p in persons), default=keep.last_seen)
    first = min((p.created_at for p in persons), default=keep.created_at)
    keep.last_seen, keep.created_at = last, first
    if not keep.label:
        keep.label = next((p.label for p in drop if p.label), "")
    keep.save(update_fields=["last_seen", "created_at", "label"])

    Person.objects.filter(pk__in=drop_ids).delete()
    Event.objects.create(person=keep, kind=Event.MERGE,
                         detail=f"{names} 흡수")

    _prune_gallery(keep)
    bus.send_command("reload_gallery", person_id=keep.pk)
    return keep, len(drop)


@transaction.atomic
def split_tracklets(tracklets, label=""):
    """선택한 트랙렛들을 떼어내 새 인물로 만든다. 오매칭 교정용.

    반환: 새로 만든 Person
    """
    tracklets = list(tracklets)
    if not tracklets:
        raise ValueError("트랙렛을 하나 이상 선택해야 한다")

    origin = tracklets[0].person
    new = Person.objects.create(
        label=label,
        created_at=min(t.start_at for t in tracklets),
        last_seen=max((t.end_at or t.start_at) for t in tracklets),
    )
    ids = [t.pk for t in tracklets]
    Snapshot.objects.filter(tracklet_id__in=ids).update(person=new)
    Tracklet.objects.filter(pk__in=ids).update(person=new)

    Event.objects.create(person=new, kind=Event.SPLIT,
                         detail=f"{origin} 에서 분리")

    for p in (new, origin):
        _prune_gallery(p)
        bus.send_command("reload_gallery", person_id=p.pk)
    return new


def _prune_gallery(person, limit=None):
    """갤러리가 무한정 커지지 않게 품질 상위 N장만 남긴다."""
    from .models import RuntimeConfig
    limit = limit or RuntimeConfig.get().max_gallery
    keep_ids = list(person.snapshots.order_by("-score")
                    .values_list("pk", flat=True)[:limit])
    stale = person.snapshots.exclude(pk__in=keep_ids)
    for s in stale:
        s.image.delete(save=False)          # 파일도 같이 지운다
    stale.delete()


@transaction.atomic
def delete_persons(persons):
    """인물 삭제. 크롭 파일까지 정리한다."""
    n = 0
    for p in persons:
        for s in p.snapshots.all():
            s.image.delete(save=False)
        p.delete()
        n += 1
    return n
