from __future__ import unicode_literals

import collections
import io
import json
import os
import shutil
import socket


from .util import (
    options,
    Option,
    parse_date,
    ProgressBar,
    TableSizeProgressBar,
)


@options()
def action_ipp_usage_phases(args, config, db, wdb):
    for pdef in config['phases']:
        time_q = "create_time >= '%s 0:00' AND create_time <= '%s 23:59'" % (
            pdef['startdate'], pdef['enddate'])
        where_q = ' WHERE creator_id != 1 AND delete_time IS NULL AND ' + time_q

        proposal_count = db.simple_query(
            'SELECT COUNT(*) FROM delegateable' + where_q + ' AND type="proposal"')[0]
        comment_count = db.simple_query(
            'SELECT COUNT(*) FROM comment' + where_q)[0]
        vote_raw_count = db.simple_query(
            'SELECT COUNT(*) FROM vote WHERE ' + time_q + ' AND user_id != 1')[0]
        vote_count = db.simple_query(
            'SELECT COUNT(DISTINCT user_id, poll_id) FROM vote WHERE ' + time_q + ' AND user_id != 1')[0]

        print('%s: %d proposals, %d comments, %d raw votes, %d votes' % (
            pdef['name'], proposal_count, comment_count, vote_raw_count,
            vote_count))


class ReverseResolver(object):
    _MOBILE_ISPS = {
        'vodafone.de',
        'd1-online.com',
    }

    def __init__(self):
        self._cache = {}
        self._cache_fn = os.path.join('.cache', 'rr_ip_table.json')

    def __enter__(self):
        try:
            with io.open(self._cache_fn, encoding='utf-8') as inf:
                self._cache = json.load(inf)
        except IOError:
            pass
        return self

    def __exit__(self, exc_type, value, traceback):
        if exc_type is not None:
            return
        cache_dir = os.path.dirname(self._cache_fn)
        if not os.path.exists(cache_dir):
            shutil.makedirs(cache_dir)
        tmp_fn = self._cache_fn + '.tmp'
        with io.open(tmp_fn, 'w', encoding='utf-8') as outf:
            json.dump(self._cache, outf)
        os.rename(tmp_fn, self._cache_fn)

    def resolve(self, ip):
        if ip.startswith('134.99.'):
            return 'hhu'
        if ip.startswith('134.94.'):
            return 'fzj'
        if ip in self._cache:
            return self._cache[ip]

        flipped_ip = '.'.join(reversed(ip.split('.')))
        arpa_dn = flipped_ip + '.in-addr.arpa'
        import DNS
        dns_response = DNS.DnsRequest(arpa_dn, qtype='PTR').req()
        if dns_response.answers and 'data' in dns_response.answers[0]:
            domain = dns_response.answers[0]['data']
        else:
            domain = 'unknown'
        self._cache[ip] = domain
        return domain

    def resolve_isp(self, ip):
        domain = self.resolve(ip)
        short_domain = '.'.join(domain.split('.')[-2:])
        return short_domain

    def resolve_class(self, ip):
        isp = self.resolve_isp(ip)
        if isp in self._MOBILE_ISPS:
            return 'mobile'
        elif isp in ('hhu', 'fzj'):
            return 'uni'
        return 'home'


@options()
def action_ipp_usage_stats(args, config, db, wdb):
    db.execute('''
        SELECT
            _sessions.session_id,
            analysis_requestlog.ip_address,
            analysis_requestlog.user_agent
        FROM analysis_requestlog
        JOIN (
            SELECT
                session_id,
                MIN(request_id) as min_request_id
            FROM analysis_session_requests
            GROUP BY session_id
        ) _sessions
        ON _sessions.min_request_id = analysis_requestlog.id
    ''')
    
    mobile_count = 0
    count = 0
    ips = []
    for session_id, ip, ua in db:
        if 'mobile' in ua.lower():
            mobile_count += 1
        ips.append(ip)
        count += 1
    print('Mobile devices were used in %d of %d sessions (%d %%)' % (
        mobile_count, count, int(round(mobile_count * 100 / count))))

    ip_types = collections.Counter()
    with ReverseResolver() as rr:
        for ip in ips:
            ip_types[rr.resolve_class(ip)] += 1
    print('Location: ' + ', '.join(
        '%s: %d' % (ltype, lcount) for ltype, lcount in ip_types.most_common()))
