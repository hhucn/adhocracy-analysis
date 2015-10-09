def get_status_groups(db):
    """ Returns a dictionary user_id => Statusgruppe """

    def get_group_by_badges(badges):
        for b in ('Professor/in / PD', 'Postdoktorand/in'):
            if b in badges:
                return b
        return 'Andere'

    res = {}
    uids = db.simple_query('SELECT id FROM user')
    for uid in uids:
        badges = db.simple_query('''
            SELECT badge.title FROM badge, user_badges
            WHERE badge.id = user_badges.badge_id AND
                  user_badges.user_id = %s''', (uid,))
        res[uid] = get_group_by_badges(badges)
    return res

