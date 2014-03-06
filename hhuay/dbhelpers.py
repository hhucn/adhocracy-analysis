import re


def get_user_map(db):
    db.execute('SELECT id, user_name, email FROM user')
    res = {}
    for row in db:
        user_id, user_name, email = row
        res[user_name] = user_id
        res[email] = user_id
    return res


_user_rex = re.compile(r'[a-f0-9]{40}([^!]+)!userid_type:unicode')


def extract_user_from_cookies(cookies, default=None):
    m = _user_rex.search(cookies)
    if m:
        return m.group(1)
    return default
