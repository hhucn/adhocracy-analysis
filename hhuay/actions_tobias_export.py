from __future__ import unicode_literals

import collections
import functools
import itertools
import json
import pickle
import os.path
import re
import time

from .dbhelpers import (
    DBConnection,
)

from .util import (
    extract_user_from_cookies,
    options,
    Option,
    TableSizeProgressBar,
)
from . import xlsx

SORTORDER_MAP = {
    '1': '-create_time',
    '2': 'order.title',
    '3': '-order.proposal.controversy',
    '4': '-order.proposal.mixed',
    '5': '-order.newestcomment',
    '6': '-order.proposal.support',
}

User = collections.namedtuple(
    'User',
    ['id', 'textid', 'name', 'gender', 'badges', 'proposal_sort_order'])

def _format_timestamp(ts):
    st = time.gmtime(ts)
    return time.strftime('%Y-%m-%d %H:%M:%S', st)

def _get_anonym_user_id(user_name, cookie, user_dict, user_id_dict):
    key = cookie

    # Is registered user?
    if user_name:
        key = user_name
    elif cookie in user_dict:
        key = user_dict[cookie]
    
    if key in user_id_dict:
        return user_id_dict[key]
    else:
        n = len(user_id_dict.values()) + 1
        user_id_dict[key] = n
        return n
    
class IPAnonymizer(object):
    def __init__(self):
        self._anonymized = {}
        self._all_values = set()

    def __call__(self, ip):
        if ip in self._anonymized:
            return self._anonymized[ip]
        first, second, _, _ = ip.split('.')

        for i in itertools.count(start=1):
            s = '%s.%s.256.%d' % (first, second, i)
            if s not in self._all_values:
                break

        self._anonymized[ip] = s
        self._all_values.add(s)
        return s

proposal_rex = re.compile(r'''(?x)^
    (?:
        (?P<is_stats>
            (?:/i/[a-z]+)?
            /stats/on_page\?
            page=https%3A%2F%2Fnormsetzung.cs.uni-duesseldorf.de%2Fi%2F[a-z]+%2Fproposal%2F
        )|
        /i/[a-z]+/proposal/
    )
    (?P<proposal_id>[0-9]+)
    -.*
''')

read_comments_rex = re.compile(r'''(?x)
    (?:/i/[a-z]+)?/stats/read_comments\?
    path=.*?%2Fproposal%2F(?P<proposal_id>[0-9]+)-
''')


class ViewStats(object):
    __slots__ = (
        'request_timestamps',
        'duration',
        'voted',
        'read_comments_count',
        'changed_vote',
        'rated_comment_count',
        'comments_written',
        'comments_written_length',
        'comment_count_on_access',
        'comment_proposal_size_on_access',
        'pro_votes_on_access',
        'contra_votes_on_access'
    )

    def __init__(self):
        self.request_timestamps = []
        self.duration = 0
        self.voted = 0
        self.read_comments_count = 0
        self.changed_vote = 0
        self.rated_comment_count = 0
        self.comments_written = 0
        self.comments_written_length = 0
        self.comment_count_on_access = 0
        self.comment_proposal_size_on_access = 0
        self.pro_votes_on_access = 0
        self.contra_votes_on_access = 0

def calc_view_stats(session, db):
    by_proposal = collections.defaultdict(ViewStats)
    
    for r in session.requests:
        # Comments read in this request?
        m = read_comments_rex.match(r.request_url)
        if m:
            proposal_id = int(m.group('proposal_id'))
            vdata = by_proposal[proposal_id]
            if not vdata.request_timestamps:
                vdata.request_timestamps.append(r.access_time)
            #print('Counting %r' % (r,))
            vdata.read_comments_count += 1

        # Update session duration
        m = proposal_rex.match(r.request_url)
        if m:
            proposal_id = int(m.group('proposal_id'))
            vdata = by_proposal[proposal_id]
            if not m.group('is_stats') or not vdata.request_timestamps:
                vdata.request_timestamps.append(r.access_time)
            vdata.duration = max(
                vdata.duration, r.access_time - vdata.request_timestamps[0])
        
    # Calculate proposal based variables
    for proposal_id in by_proposal:
        vdata = by_proposal[proposal_id]
        first_access_session = vdata.request_timestamps[0] #When did user first access THIS proposal during THIS session?
        last_access_session = vdata.request_timestamps[-1] #When did user last access THIS proposal during THIS session?
        #print('[%d] first access: %d' % (proposal_id, first_access_session))
        
        # Number of comments on access
        vdata.comment_count_on_access = db.simple_query('''SELECT count(*)
        FROM comment
        WHERE topic_id =
            (SELECT description_id
            FROM proposal
            WHERE id=%d
            )
        AND (create_time < FROM_UNIXTIME(%d))
        AND (delete_time IS NULL OR delete_time > FROM_UNIXTIME(%d));
        ''' % (proposal_id, first_access_session, first_access_session))[0]
            
        # number of pro and contra votes when user accessed proposal page (i.e. before she voted)
        db.execute('''SELECT vote.user_id, vote.orientation
        FROM vote
        INNER JOIN 
            (SELECT user_id, MAX(create_time) AS last_create_time FROM
                (SELECT *
                FROM vote
                WHERE (UNIX_TIMESTAMP(create_time) < %d)
                AND poll_id = (SELECT id FROM poll WHERE subject LIKE '@[proposal:%d]')
                ) AS proposal_vote
            GROUP BY user_id
            ) AS user_proposal_vote
        ON (vote.user_id = user_proposal_vote.user_id and vote.create_time = user_proposal_vote.last_create_time);
        ''' % (first_access_session, proposal_id))
        number_votes_pro = 0
        number_votes_con = 0
        for row in db:
            user_id, orientation = row[0],row[1]
            if orientation == 1:
                number_votes_pro += 1
            if orientation == -1:
                number_votes_con += 1
        #print('[%d] pro votes on access: %d' % (proposal_id, number_votes_pro))
        #print('[%d] con votes on access: %d' % (proposal_id, number_votes_con))
        vdata.pro_votes_on_access = number_votes_pro
        vdata.contra_votes_on_access = number_votes_con
        
        # total number of characters of THIS proposal and comments for THIS proposal when THIS user first accessed the proposal page in THIS session
        comment_length_raw = db.simple_query('''SELECT SUM(CHAR_LENGTH(revision.text))
        FROM revision
        INNER JOIN 
            (SELECT comment_id, MAX(create_time) AS last_create_time FROM
                (SELECT *
                FROM revision
                WHERE comment_id IN
                    (SELECT id
                    FROM comment
                    WHERE topic_id =
                        (SELECT description_id
                        FROM proposal
                        WHERE id=%d
                        )
                    AND (create_time < FROM_UNIXTIME(%d))
                    AND (delete_time IS NULL OR delete_time > FROM_UNIXTIME(%d))
                    ) 
                AND create_time < FROM_UNIXTIME(%d)
                ) AS revision_validated
            GROUP BY comment_id
            ) AS revision_validated_filtered
        ON (revision.comment_id = revision_validated_filtered.comment_id and revision.create_time = revision_validated_filtered.last_create_time)
        ''' % (proposal_id, first_access_session, first_access_session, first_access_session))[0]
        comment_length = comment_length_raw if (comment_length_raw is not None) else 0
        #print('[%d] comment_length: %d' % (proposal_id, comment_length));
        
        db.execute('''SELECT CHAR_LENGTH(text.title), CHAR_LENGTH(text.text)
        FROM text
        INNER JOIN 
            (SELECT page_id, MAX(create_time) AS last_create_time FROM
                (SELECT *
                FROM text
                WHERE page_id =
                    (SELECT description_id
                    FROM proposal
                    WHERE id=%d
                    )
                AND create_time < FROM_UNIXTIME(%d)
                ) AS text_validated
            GROUP BY page_id
            ) AS text_validated_filtered
        ON (text.page_id = text_validated_filtered.page_id and text.create_time = text_validated_filtered.last_create_time);
        ''' % (proposal_id, first_access_session))
        proposal_length_title = 0
        proposal_length_text = 0
        for row in db:
            proposal_length_title = row[0]
            proposal_length_text = row[1]
        #print('[%d] proposal_length (title, text): %d, %d' % (proposal_id, proposal_length_title, proposal_length_text));
        vdata.comment_proposal_size_on_access = comment_length + proposal_length_title + proposal_length_text
        
        # total number of characters in comments for THIS proposal written by THIS user DURING THIS session
        comment_written_count = 0
        comment_written_length = 0
        if session.user_name:
            db.execute('''SELECT COUNT(*), SUM(CHAR_LENGTH(text))
            FROM revision
            WHERE comment_id in
                (SELECT id
                FROM comment
                WHERE topic_id =
                    (SELECT description_id
                    FROM proposal
                    WHERE id=%d
                    )
                )
            AND user_id = (SELECT id FROM user WHERE user_name = '%s')
            AND (create_time <= FROM_UNIXTIME(%d))
            AND (create_time >= FROM_UNIXTIME(%d));
            ''' % (proposal_id, session.user_name, last_access_session, first_access_session))
            for row in db:
                comment_written_count = row[0]
                comment_written_length = row[1] if (row[1] is not None) else 0
        #print('[%d] comment_written_count: %d' % (proposal_id, comment_written_count))
        #print('[%d] comment_written_length: %d' % (proposal_id, comment_written_length))
        vdata.comments_written_length = comment_written_length
        vdata.comments_written = comment_written_count
        
        # vote result by THIS user for THIS proposal after THIS session: +1 for approval, -1 for disapproval (if voted multiple times, this always carries the latest vote)
        vote_after_session = 0
        if session.user_name:
            vote_after_session_raw = db.simple_query('''SELECT vote.orientation
            FROM vote
            INNER JOIN 
                (SELECT user_id, MAX(create_time) AS last_create_time
                FROM vote
                WHERE (UNIX_TIMESTAMP(create_time) <= %d)
                AND poll_id = (SELECT id FROM poll WHERE subject LIKE '@[proposal:%d]')
                AND user_id = (SELECT id FROM user WHERE user_name = '%s')
                ) AS vote_filtered
            ON (vote.user_id = vote_filtered.user_id and vote.create_time = vote_filtered.last_create_time);
            ''' % (last_access_session, proposal_id, session.user_name))
            vote_after_session = vote_after_session_raw[0] if vote_after_session_raw else 0;
        #print('[%d] user\'s vote after session: %d' % (proposal_id, vote_after_session))
        vdata.voted = vote_after_session
        
        # Did THIS user vote on THIS proposal during THIS session?
        did_vote = 0
        if session.user_name:
            did_vote_raw = db.simple_query('''SELECT id
            FROM vote
            WHERE (UNIX_TIMESTAMP(create_time) <= %d)
            AND (UNIX_TIMESTAMP(create_time) >= %d)
            AND poll_id = (SELECT id FROM poll WHERE subject LIKE '@[proposal:%d]')
            AND user_id = (SELECT id FROM user WHERE user_name = '%s');
            ''' % (last_access_session, first_access_session, proposal_id, session.user_name))
            did_vote = 1 if did_vote_raw else 0;
        #print('[%d] did user vote during this session? %d' % (proposal_id, did_vote))
        vdata.changed_vote = did_vote
        
        # number of comments for THIS proposal that were rated by THIS user DURING THIS session
        comments_rated = 0
        if session.user_name:
            comments = db.simple_query('''SELECT id
            FROM comment
            WHERE topic_id =
                (SELECT description_id
                FROM proposal
                WHERE id=%d
                );
            ''' % (proposal_id,))
            for comment_id in comments:
                comments_rated_raw = db.simple_query('''SELECT COUNT(*)
                FROM vote
                WHERE (UNIX_TIMESTAMP(create_time) <= %d)
                AND (UNIX_TIMESTAMP(create_time) >= %d)
                AND poll_id = (SELECT id FROM poll WHERE subject LIKE '@[comment:%d]')
                AND user_id = (SELECT id FROM user WHERE user_name = '%s');
                ''' % (last_access_session, first_access_session, comment_id, session.user_name))[0]
                comments_rated += comments_rated_raw
        #print('[%d] number of votes for comments during this session: %d' % (proposal_id, comments_rated))
        vdata.rated_comment_count = comments_rated
        

    return by_proposal

def _is_external(ip):
    return not (ip.startswith('134.99.') or ip.startswith('134.94.'))

def _request_counter(rex):
    re_obj = re.compile(rex)

    def count(session):
        return sum(
            1 for r in session.requests
            if '/stats/' not in r.request_url and
               re_obj.match(r.request_url) is not None)

    return count

USER_HEADER = [
    'User ID',
    'Array of Badges (JSON)',
    'Female',
    'StatusProf',
    'StatusPostdoc',
    'StatusOther',
    'StatusFR',
    'StatusHA',
]

def read_users(db):
    """ Return a dictionary of textids mapping to tuples
        (User object, Cell values) """

    user_filter = 'user.delete_time IS NULL AND user.id != 1'

    db.execute('''SELECT
        user.id,
        user.user_name,
        user.display_name,
        user.gender,
        user.proposal_sort_order
    FROM user
    WHERE %s
    ORDER BY id;''' % user_filter)
    users = {
        row[0]: User(row[0], row[1], row[2], row[3], set(), row[4])
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

    user_info = {}
    for u in users.values():
        assert u.gender in ('m', 'f')
        gender_code = 0 if u.gender == 'm' else 1
        status_prof = int("Professor/in / PD" in u.badges)
        status_postdoc = int("Postdoktorand/in" in u.badges)
        status_other = int("Andere" in u.badges)
        status_fakrat = int("FakultÃ¤tsrat" in u.badges)
        status_habilausschuss = int("Habilitationsausschuss" in u.badges)

        assert sum([status_prof, status_postdoc, status_other]) == 1
        cells = [
            u.id,
            json.dumps(sorted(u.badges)),
            gender_code,
            status_prof,
            status_postdoc,
            status_other,
            status_fakrat,
            status_habilausschuss,
        ]
        user_info[u.textid] = (u, cells)
    return user_info

def export_users(ws, db):
    ws.freeze_panes(1, 0)
    ws.write_header(USER_HEADER)
    sorted_uis = sorted(read_users(db).values(), key=lambda ui: ui[0].id)
    sorted_rows = [ui[1] for ui in sorted_uis]
    ws.write_rows(sorted_rows)

Comment = collections.namedtuple(
    'Comment', ['id', 'creator_id', 'create_time', 'revision_id', 'text'])

def read_comments(db):
    db.execute('''SELECT
        comment.id,
        comment.creator_id,
        UNIX_TIMESTAMP(comment.create_time),
        revision.id,
        revision.text
    FROM comment, revision
    WHERE comment.delete_time IS NULL AND
        comment.id = revision.comment_id
    ''')

    row_by_id = {}
    for row in db:
        comment_id = row[0]
        row_by_id[comment_id] = row

    res = {}
    for row in row_by_id.values():
        user_id = row[1]
        res.setdefault(user_id, []).append(Comment(*row))
    return res

Proposal = collections.namedtuple(
    'Proposal', ['id', 'title', 'visible', 'instance', 'create_time', 'creator_name'])

def read_proposals(db):
    db.execute('''SELECT
        proposal.id,
        delegateable.label,
        delegateable.delete_time,
        instance.key,
        UNIX_TIMESTAMP(delegateable.create_time),
        user.user_name
    FROM proposal, delegateable, instance, user
    WHERE proposal.id = delegateable.id AND
          delegateable.instance_id = instance.id AND
          user.id = delegateable.creator_id
    ORDER BY delegateable.create_time ASC
    ''')
    proposals = []
    for row in db:
        proposal_id, title, delete_time, instance_key, create_time, creator_name = row
        visible = 1 if delete_time is None else 0
        proposals.append(Proposal(
            proposal_id, title, visible, instance_key, create_time, creator_name))
    return proposals

def export_proposals(ws, db):
    ws.freeze_panes(1, 0)
    ws.write_header(['id', 'visible', 'instance', 'created', 'title'])
    proposals = read_proposals(db)
    ws.write_rows([[
        p.id,
        p.visible,
        p.instance,
        _format_timestamp(p.create_time),
        p.title
    ] for p in proposals])

class Session(object):
    __slots__ = 'tracking_cookie', 'session_id', 'requests', 'user_name', 'length', 'start_time', 'end_time'

    def __init__(self):
        self.tracking_cookie = None
        self.session_id = None
        self.requests = []
        self.user_name = None
        self.length = None
        self.start_time = None
        self.end_time = None
Request = collections.namedtuple('Request', [
    'id', 'ip', 'access_time', 'request_url', 'cookies', 'user_agent',
    'method'])

def _is_admin(s, user_dict):
    if s.user_name == 'admin':
        return 1
    if s.tracking_cookie and (s.tracking_cookie in user_dict) and user_dict[s.tracking_cookie] == 'admin':
        return 1
    return 0
    
def read_sessions(db, config, user_dict):
    sessions = []
    
    print('Reading session data from database ...')
    db.execute('''SELECT
        analysis_session.id AS session_id,
        analysis_session.first_update_timestamp AS start_time,
        analysis_session.last_update_timestamp AS end_time,
        analysis_session.tracking_cookie AS tracking_cookie,
        analysis_session_length.session_length AS length
    FROM
        analysis_session, analysis_session_length
    WHERE
        analysis_session_length.session_id = analysis_session.id
    ORDER BY
        tracking_cookie, session_id
    ;''')
    
    # Get session ids and access time infos
    for row in db:
        (session_id, start_time, end_time, tracking_cookie, length) = row
        s = Session()
        s.session_id = session_id
        s.start_time = start_time
        s.end_time = end_time
        s.tracking_cookie = tracking_cookie
        s.length = length
        sessions.append(s)
            
    bar = TableSizeProgressBar(
        db, 'analysis_session', 'Reading sessions')
    for s in sessions:
        bar.next()
        
        # Get session users
        db.execute('''SELECT user_sid
        FROM analysis_session_users
        WHERE session_id=%s
        ;''' % s.session_id )
        
        # Associate user names to sessions and to cookies 
        for row in db:
            (user_name,) = row
            if user_name != None:
                s.user_name = user_name
                if s.tracking_cookie:
                    user_dict[s.tracking_cookie] = user_name
        
        # Get Session requests
        request_ids = db.simple_query('SELECT request_id FROM analysis_session_requests WHERE session_id=%d' % s.session_id )
        
        for request_id in request_ids:
            db.execute('''SELECT access_time, ip_address, request_url, cookies, user_agent, method
            FROM analysis_requestlog_undeleted
            WHERE id=%d
            ORDER BY access_time ASC
        ;''' % request_id )
            for row in db:
                (access_time, ip_address, request_url, cookies, user_agent, method) = row
                s.requests.append(Request(request_id, ip_address, access_time, request_url, cookies, user_agent, method))
    
    # Remove sessions associated with admin
    print('\nsessions total: %d' % len(sessions))
    sessions[:] = [s for s in sessions if not _is_admin(s, user_dict)]
    print('sessions without admin: %d' % len(sessions))
    
    return sessions

def export_sessions(args, ws, db, config):
    print('Reading database')
    proposals = read_proposals(db)
    users = read_users(db)
    all_comments = read_comments(db)

    user_dict = {} # Maps tracking cookies to associated user_names
    sessions = read_sessions(db, config, user_dict)
    ipa = IPAnonymizer()
    
    print('Processing sessions ...')

    ws.freeze_panes(1, 0)
    headers = [
        'TrackingCookie', 'UserName', #TODO: nur zum Testen!!
        'SessionId', 'AnonymUserId ', 'AccessFrom', 'Anonymized IP Address', 'Device Type',
        'LoginFailures',
        'SessionStart_Date', 'SessionStart', 'SessionEnd_Date', 'SessionEnd',
        'SessionDuration',
        'NavigationCount', 'VotedCount', 'CommentsWritten', 'CommentsLength',
        'Resorted (JSON)',
        'ProposalsViewed', 'ViewedKnowledgeBase',
    ]
    
    headers += USER_HEADER
    if args.include_proposals:
        for i, p in enumerate(proposals):
            proposal_templates = [
                'V%d_ID',
                'V%d_Name',
                'V%d_Active',
                'V%d_Created',
                'V%d_CreatedByThisUser',
                'V%d_RequestTimestamps',
                'V%d_Duration',
                'V%d_Voted',
                'V%d_CommentsRead',
                'V%d_ChangedVote',
                'V%d_RatedCommentCount',
                'V%d_CommentsWritten',
                'V%d_CommentsWrittenLength',
                'V%d_CommentCountOnAccess',
                'V%d_CommentProposalSizeOnAcces',
                'V%d_ProVotesOnAccess',
                'V%d_ContraVotesOnAccess',
            ]
            headers += [h % i for h in proposal_templates]
    
    ws.write_header(headers)

    login_failures = _request_counter(r'/+post_login\?_login_tries=0')
    navigation_count = _request_counter(r'/')
    vote_count = _request_counter(r'/.*/rate\.')
    proposal_sort_order_re = re.compile(r'&proposals_sort=([0-9]+)')
    access_knowledge_base_rex = re.compile(r'/i/grundsaetze/outgoing_link/824893fea3ed4bc0c9789e8d2fd6eb6b8f7c1ab635ec800a1edfff4f740bf837!aHR0cDovL3d3dy5waGlsby5oaHUuZGUvYWthZGVtaXNjaGUtcXVhbGlmaXppZXJ1bmcvaGFiaWxpdGF0aW9uLmh0bWw=\?')

    user_id_dict = {}
    for row_num, s in enumerate(sessions, start=1):
        comments = []
        user_name = s.user_name
        if user_name and (user_name in users):
            ui, user_rows = users[user_name]
            user_row = user_rows
            
            if ui.id in all_comments:
                comments = [
                    c for c in all_comments[ui.id]
                    if s.requests[0].access_time <= c.create_time - 2 and
                    c.create_time <= s.requests[-1].access_time + 2
                ]
        else:
            ui = None
            user_row = ['anonymous',None,None,None,None,None,None,None]
        
        # Go through all requests of this session...
        resorted = []
        did_access_knowledge_base = 0
        proposals_viewed = []
        for r in s.requests:
            # Resorted in this request?
            m = proposal_sort_order_re.search(r.request_url)
            if m:
                resorted.append(SORTORDER_MAP[m.group(1)])
            
            # Did click external link "Habilitationsordnung" in this request?
            m2 = access_knowledge_base_rex.match(r.request_url)
            if m2:
                did_access_knowledge_base += 1
                
            # Update list of proposals viewed during this session
            m3 = proposal_rex.match(r.request_url)
            if m3:
                proposal_id = int(m3.group('proposal_id'))
                if not m3.group('is_stats') and ((not proposals_viewed) or (proposal_id != proposals_viewed[-1])):
                    proposals_viewed.append(proposal_id)

        
        proposals_row = []
        if args.include_proposals:
            view_stats = calc_view_stats(s, db)
            for p in proposals:
                created_by_this_user = 1 if (p.creator_name == s.user_name) else 0
                proposals_row.extend([
                    p.id,
                    p.title,
                    p.visible,
                    p.create_time,
                    created_by_this_user,
                ])
                if p.id in view_stats:
                    vs = view_stats[p.id]
                    proposals_row.extend([
                        json.dumps(vs.request_timestamps), #OK
                        vs.duration, #OK
                        vs.voted,
                        vs.read_comments_count, #OK
                        vs.changed_vote,
                        vs.rated_comment_count,
                        vs.comments_written,
                        vs.comments_written_length,
                        vs.comment_count_on_access,
                        vs.comment_proposal_size_on_access,
                        vs.pro_votes_on_access,
                        vs.contra_votes_on_access,
                    ])
                else:
                    proposals_row.extend([
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None
                    ])

        row = [
            s.tracking_cookie, #TODO: Nur zum Testen!!!
            s.user_name, #TODO: Nur zum Testen!!!
            s.session_id,
            _get_anonym_user_id(s.user_name, s.tracking_cookie, user_dict, user_id_dict),
            'external' if _is_external(s.requests[0].ip) else 'university',
            ipa(s.requests[0].ip),
            'mobile' if 'mobile' in s.requests[0].user_agent else 'regular', #TODO besser abfragen! Das hier erwischt nicht alle mobilen Geräte.
            login_failures(s),
            _format_timestamp(s.start_time),
            s.start_time,
            _format_timestamp(s.end_time),
            s.end_time,
            s.end_time - s.start_time,
            navigation_count(s),
            vote_count(s),
            len(comments),
            sum(len(c.text) for c in comments),
            json.dumps(resorted) if resorted else None,
            json.dumps(proposals_viewed),
            did_access_knowledge_base,
        ] + user_row + proposals_row
        
        ws.write_row(row_num, row)

@options([Option(
    '--output',
    metavar='FILENAME',
    dest='out_fn',
    help='Output filename'
), Option(
    '--timeout',
    dest='timeout',
    help='Session timeout in seconds',
    type=int,
    default=60 * 60
), Option(
    '--include-proposals',
    dest='include_proposals',
    action='store_true',
    help='Include proposals in session table',
)])
def action_tobias_export(args, config, db, wdb):
    book = xlsx.gen_doc(args.out_fn, ['Sessions', 'Benutzer', 'Proposals'])

    export_sessions(args, book.worksheets_objs[0], db, config)
    export_users(book.worksheets_objs[1], db)
    export_proposals(book.worksheets_objs[2], db)

    book.close()
