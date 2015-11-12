import collections

from . import util
from .util import NoProgress
from .filters import filter_config_dates


Request = collections.namedtuple(
    'Request',
    ('time', 'ip', 'path', 'cookies', 'user_agent')
)

User = collections.namedtuple(
    'User',
    ('name', 'email', 'badges')
)

Vote = collections.namedtuple(
    'Vote',
    ('id', 'subject', 'time', 'orientation', 'user')
)

Comment = collections.namedtuple(
    'Comment',
    ('id', 'time', 'user')
)

Proposal = collections.namedtuple(
    'Proposal',
    ('id', 'time', 'user')
)


def get_requests_from_db(db):
    db.execute(
        '''SELECT
            UNIX_TIMESTAMP(requestlog.access_time), requestlog.ip_address, requestlog.request_url, requestlog.cookies, requestlog.user_agent
            FROM requestlog''')
    for row in db:
        yield Request(*row)


def get_votes_from_db(db):
    db.execute(
        '''SELECT
            vote.id, poll.subject, UNIX_TIMESTAMP(vote.create_time),
            vote.orientation, user.user_name
            FROM vote, poll, user
            WHERE vote.poll_id = poll.id and vote.user_id = user.id''')
    for row in db:
        yield Vote(*row)


def get_proposals_from_db(db):
    db.execute(
        '''SELECT
            proposal.id, UNIX_TIMESTAMP(delegateable.access_time), user.user_name
            FROM proposal, delegateable, user
            WHERE proposal.id = delegateable.id and delegateable.creator_id = user.id
                and delegateable.delete_time IS NULL''')
    for row in db:
        yield Proposal(*row)


def get_comments_from_db(db):
    db.execute(
        '''SELECT
            comment.id, UNIX_TIMESTAMP(comment.create_time), user.user_name
            FROM comment, delegateable, user
            WHERE comment.id = delegateable.id and comment.creator_id = user.id
                and comment.delete_time IS NULL''')
    for row in db:
        yield Comment(*row)


Action = collections.namedtuple('Action', ['key', 'rl_value', 'db_value'])


def get_all_actions(config, db):
    METRICS = [
        ('logged_in', lambda row: True, lambda *args: None),
        (
            'vote',
            lambda row: '/rate' in row[2],
            get_votes_from_db
        ),
        (
            'comment',
            lambda row: row[2].endswith('/comment'),
            get_comments_from_db
        ),
        (
            'proposal',
            lambda row: row[2].endswith('/proposal'),
            get_proposals_from_db
        ),
    ]

    # make a list of (time, user) for each action
    db.execute(
        '''SELECT access_time, user_sid, request_url, method
        FROM analysis_requestlog_combined
        WHERE user_sid IS NOT NULL AND user_sid != 'admin'
        ORDER BY access_time''')
    all_requests = list(db)

    matching_requests = dict(
        (mname,
         [(row[0], row[1]) for row in all_requests if mfunc(row)])
        for mname, mfunc, _ in METRICS)

    return [
        Action(
            mname,
            matching_requests[mname],
            list(filter_config_dates(dbfunc(db), config)),
        )
        for mname, _, dbfunc in METRICS]


