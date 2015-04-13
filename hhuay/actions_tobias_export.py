from __future__ import unicode_literals

import collections
import json

from .util import (
    options,
    Option,
)
from . import xlsx


User = collections.namedtuple('User', ['id', 'name', 'gender', 'badges'])


def export_users(ws, db):
    ws.write_header([
        'User ID', 'Array of Badges (JSON)', 'Female',
        'StatusProf', 'StatusCoordinator', 'StatusWiMi', 'StatusNiWiMi',
        'StatusPhD', 'StatusExtern', 'StatusStudent'])

    user_filter = 'user.delete_time IS NULL AND user.id != 1'

    db.execute('''SELECT
        user.id,
        user.display_name,
        user.gender
    FROM user
    WHERE %s
    ORDER BY id;''' % user_filter)
    users = {
        row[0]: User(row[0], row[1], row[2], set())
        for row in db
    }

    db.execute('''SELECT
        user.id,
        badge.title
    FROM user, user_badges, badge
    WHERE %s AND
        badge.id = user_badges.badge_id AND
        user.id = user_badges.user_id;''' % user_filter)
    for row in db:
        users[row[0]].badges.add(row[1])

    sorted_users = sorted(users.values(), key=lambda u: u.id)
    for i, u in enumerate(sorted_users, start=1):
        assert u.gender in ('m', 'f')
        gender_code = 0 if u.gender == 'm' else 1
        status_prof = "Professor/in" in u.badges
        status_coordinator = "KoNo-Projekt" in u.badges
        status_wi_mi = "Mittelbau" in u.badges
        status_ni_wi_mi = (
            "Weitere Mitarbeiterinnen und Mitarbeiter" in u.badges)
        status_phd = "Doktorand/in" in u.badges
        status_extern = 'Nicht erfasst' if status_phd else 0
        status_student = "Studierende" in u.badges

        assert sum(
            [status_prof, status_wi_mi, status_ni_wi_mi, status_phd,
             status_student]) <= 1

        ws.write_row(i, [
            u.id, json.dumps(sorted(u.badges)), gender_code,
            status_prof, status_coordinator, status_wi_mi, status_ni_wi_mi,
            status_phd, status_extern, status_student])


@options([Option(
    '--output',
    metavar='FILENAME',
    dest='out_fn',
    help='Output filename')
])
def action_tobias_export(args, config, db, wdb):
    book = xlsx.gen_doc(args.out_fn, ['Benutzer'])

    export_users(book.worksheets_objs[0], db)

    book.close()
