from .util import db_simple_query


def get_status_groups(db):
    """ Returns a dictionary user_id => Statusgruppe """

    def get_group_by_badges(badges):
        for b in ('Professor/in', 'Mittelbau', 'Doktorand/in'):
            if b in badges:
                return b
        return 'Sonstige'

    res = {}
    uids = db_simple_query(db, 'SELECT id FROM user')
    for uid in uids:
        badges = db_simple_query(db, '''
            SELECT badge.title FROM badge, user_badges
            WHERE badge.id = user_badges.badge_id AND
                  user_badges.user_id = %s''', (uid,))
        res[uid] = get_group_by_badges(badges)
    return res

