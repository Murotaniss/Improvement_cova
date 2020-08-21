import base64
import datetime
import urllib.parse
import unittest

def get_birth(uid):
    ''' uuidの生起時刻を取得する
        idfa/anid等のso cookieではないuuidの場合はNoneが返る
    '''
    uid = str(uid)
    uid = uid.replace('.', '+').replace('-', '/')
    if len(uid) != 24 or uid.find(':') != -1:
        return None

    try:
        byte_seq = base64.b64decode(uid)
    except:
        return -1
    if len(byte_seq) < 4:
        return None

    result = 0
    for i in range(4):
        result += (byte_seq[i] & (0xff)) << ((3 - i) << 3)

    return result

def get_device_from_os(os):
    ''' OSに応じてpc/sp/Unknownを返す
    '''
    if not os:
        return 'pc'
    os_type_to_device_type = {
        'iOS': 'sp', 'Android': 'sp', 'Mac': 'pc', 'Linux': 'pc', 'Windows': 'pc', 'PlayStation': 'game', 'Nintendo': 'game'
    }
    os_type = get_os_type(os, '')
    device_type = os_type_to_device_type.get(os_type, 'pc')
    return device_type

def get_device_from_os_mediatype(os, media_type):
    ''' OSとmedia_typeに応じてpc/sp/app/gameを返す
    '''
    if not os:
        device = 'pc'
    elif media_type == 'app':
        device = 'app'
    else:
        device = get_device_from_os(os)
    return device

def get_os_type(os, user_agent):
    ''' OSとuser_agentからiOS, Android, Windows, Mac, Linux, Unknownを返す
    '''
    if not os:
        return 'Unknown'

    os_to_device = {
        'iPhone': 'iOS', 'Android': 'Android', 'Mac': 'Mac', 'Linux': 'Linux', 'iPod': 'iOS', 'iPad': 'iOS', 'Windows': 'Windows'
    }
    short_os = os_to_device.get(os.split(' ')[0], 'Unknown')

    if short_os == 'Unknown':
        if 'PlayStation' in user_agent:
            short_os = 'PlayStation'
        elif 'Nintendo' in user_agent:
            short_os = 'Nintendo'
    return short_os

SSP_ID_MAP = {
    58722: 58722,                               # Videology(58722)
    54672: 54672,  54673: 54672,                # AppLovin(54672), AppLovin_inf(54673)
     8651:  8651,  10405:  8651,                # GMO(8651), GMO_r18(10405)
    24301: 24301,                               # AOL_MARKETPLACE(24301)
     8086:  8086,                               # OpenX(8086)
    66255: 66255,  66255: 68351,                # SpotX(68351)
    10392: 10392,  25585: 10392, 19564: 10392,  48464: 10392,
                                                # adgen(10392), adgen_inf(25585), adgen_r18(19564), AppVador(48464)
    47445: 47445,                               # Teads(47445)
    51589: 51589,  51588: 51589,                # Mopub(51588), Mopub_inf(51589)
    58705: 58705,                               # Vungle(58705)
    56007: 56007,  57429: 56007,                # AppNexus(56007), Yahoo_via_AppNexus(57429)
     8085:  8085,   8085: 44196,                # DoubleClick(8085), DoubleClick_inf(44196)
    12372: 12372,                               # PubMatic(12372)
     8392:  8392,   9918:  8392,                # AdStir(8392), AdStir_r18(9918)
    16535: 16535,                               # Rubicon(16535)
    80177: 80177,  63691: 80177,                # AJA(80177), AJA_via_BidSwitch(63691)
     8944:  8944,                               # brainy(8944)
     8338:  8338,  32605:  8338, 19411:  8338,  # Fluct(8338), Fluct_inf(32605), Fluct_r18(19411)
    59278: 59278,  66255: 59278,                # Unruly_via_BidSwitch(59278), SpotX_via_BidSwitch(66255)
    17439: 17439,  17600: 17439,                # i-mobile(17439), i-mobile_r18(17600)
    27907: 27907,                               # CA ProFit-X(27907)
     8107:  8107,  32606:  8107, 10348: 8107, 32607: 8107,
                                                # GenieeSSP(8107), GenieeSSP_inf(32606), GenieeSSP_r18(10348), GenieeSSP_r18_inf(32607)
    32603: 32603,  32604: 32603,                # BidSwitch(32603), BidSwitch_inf(32604)
     9352:  9352,                               # mediba3PAS iOS(9352)
    87424: 87424,                               # Unruly(87424)
    23109: 23109,                               # スキルアップビデオテクノロジー(23109)
     9353:  9353,                               # 3PAS android : 9353
     8391:  8391,                               # adcloud Exchange for Smartphone : 8391
    17601: 17601,                               # MicroAd COMPASS : 17601
}
def get_ssp_page_id(page_id):
    ''' page_id変換
    '''
    return SSP_ID_MAP.get(page_id, -1)

def timestamp_to_hour(ts):
    ''' timestampをhour（Integer）に変換
    '''
    ts = int(ts)
    return datetime.datetime.fromtimestamp(ts).hour

def is_pmp(common_data):
    ''' common_dataの内容からPMP判定を行う
        PMPの場合は1を、それ以外は0を返す
    '''
    if common_data.get('deal_ver2'):
        return 1
    pmp_deal_ids = common_data.get('pmp.deal_ids')
    if pmp_deal_ids and pmp_deal_ids != '[]':
        return 1
    return 0

def is_movie(common_data):
    ''' common_dataの内容から動画広告判定を行う
        動画の場合は1を、それ以外は0を返す
    '''
    if common_data.get('proto') in ['teads', 'appvador']:
        return 1
    return 0

def get_int_or_default(value, default=-1):
    '''intにcastして値を取得。Noneや殻文字の場合はデフォルト値を返す。
    '''
    if value is None or value == '':
        return default
    try:
        return int(value)
    except:
        return default

def get_int_or_zero(value):
    return get_int_or_default(value, default=0)

def get_domain_or_app_from_domain(media_type, domain, app_id):
    ''' media_typeに応じてdomainかapp_idののいずれかを返す
    '''
    if media_type == 'app' and app_id:
        return app_id
    else:
        return domain

def get_domain_or_app_from_tp(media_type, tp, app_id):
    ''' media_typeに応じてdomainかapp_idののいずれかを返す
    '''
    if media_type == 'app' and app_id:
        return app_id
    else:
        return get_domain(tp)

def get_domain_or_app(common_data):
    ''' media_typeがappの場合はbundle_idまたはtrack_idを返す
        media_typeがsiteの場合はドメイン（一部パス付き）を返す
    '''
    if common_data['media_type'] == 'app':
        if common_data.get('app_bundle_id'):
            return common_data['app_bundle_id']
        else:
            return common_data.get('app_track_id')
    else:
        return get_domain(common_data.get('tp'))


def get_domain(url):
    ''' domainの取得（一部パス付き）
    '''
    url = str(url)
    try:
        obj = urllib.parse.urlparse(url)
    except Exception as e:
        return None
    if not obj.hostname in domain_path_depth:
        return obj.hostname
    depth = domain_path_depth[obj.hostname]
    splitted_path = obj.path.rstrip('/').split('/')
    if len(splitted_path) == 0:
        return obj.hostname
    if len(splitted_path) == 1 and not splitted_path[0]:
        return obj.hostname
    return '/'.join([obj.hostname] + splitted_path[1:depth+1]).lower()

domain_path_depth = {
    'www.nikkan-gendai.com': 3,
    'a.ibbs.info': 1,
    'ameblo.jp': 1,
    'b.ibbs.info': 1,
    'blog.excite.co.jp': 1,
    'blog.fc2.com': 1,
    'blog.livedoor.com': 1,
    'blog.livedoor.jp': 1,
    'blue.ap.teacup.com': 1,
    'cyclestyle.net': 1,
    'd.hatena.ne.jp': 1,
    'fanblogs.jp': 1,
    'homepage3.nifty.com': 1,
    'ibbs.info': 1,
    'jah.jp': 1,
    'kakaku.com': 1,
    'lyze.jp': 1,
    'm.mantan-web.jp': 1,
    'm.news-postseven.com': 1,
    'm.searchina.ne.jp': 1,
    'mantan-web.jp': 1,
    'news.biglobe.ne.jp': 1,
    'plaza.rakuten.co.jp': 1,
    's.ameblo.jp': 1,
    's.kakaku.com': 1,
    'searchina.ne.jp': 1,
    'shukan.bunshun.jp': 1,
    'taishu.jp': 1,
    'www.asahi-net.or.jp': 1,
    'www.daily.co.jp': 1,
    'www.din.or.jp': 1,
    'www.i2i.jp': 1,
    'www.jprime.jp': 1,
    'www.nikkansports.com': 1,
    'www.olivenews.net': 1,
    'www.sponichi.co.jp': 1,
    'www.tokyo-sports.co.jp': 1,
    'www.zakzak.co.jp': 1}

for i in range(100):
    domain_path_depth['www{}.atwiki.jp'.format(i)] = 1
    domain_path_depth['www{}.ocn.ne.jp'.format(i)] = 1
    domain_path_depth['www{}.odn.ne.jp'.format(i)] = 1
    domain_path_depth['www{}.plala.or.jp'.format(i)] = 1

def exclude_domain_prefix(domain):
    ''' ドメイン正規化
        sp, pc, wwwなどのサブドメインの除去を行う
    '''
    if not domain:
        return None
    if not '.' in domain:
        return None
    ds = domain.split('.')
    if len(ds[0]) <= 1 or ds[0] in EXCLUDE_SUB_DOMAINS:
        domain = '.'.join(ds[1:])
    return domain

def domain_normalize(domain):
    ''' ドメイン正規化
        sp, pc, wwwなどのサブドメインの除去や不要サイトの除去を行う
    '''
    if not domain:
        return None
    if not '.' in domain:
        return None

    # 除外ドメイン
    if domain in EXCLUDE_DOMAINS:
        return None
    ds = domain.split('.')
    # 除外ドメイン（前方）
    if ds[0] in EXCLUDE_DOMAIN_PREFIXES:
        return None
    # 除外ドメイン（後方）
    if '.'.join(ds[1:]) in EXCLUDE_BASE_DOMAINS:
        return None
    for d in EXCLUDE_ENDS_WITH_DOMAINS:
        if domain.endswith(d):
            return None
    # サブドメイン除去
    if len(ds[0]) <= 1 or ds[0] in EXCLUDE_SUB_DOMAINS:
        domain = '.'.join(ds[1:])
    return domain

EXCLUDE_SUB_DOMAINS = set(['sp', 'pc', 'ad', 'ads', 'lite', 'touch', 'www'] + ['www%d' % i for i in range(100)])

EXCLUDE_DOMAINS = set([
    'sh.adingo.jp',
    'aladdin.genieesspv.jp',
    'ninja.co.jp',
    'aegate-d.openx.net',
    'microsofrefere://itunes.apple.com/app/id1345968745tadvertisingexchange.com',
    'pubads.g.doubleclick.net',
    'securepubads.g.doubleclick.net',
    'optimized-by.rubiconproject.com',
    'fluct.jp',
    'ads.mopub.com',
    'play.google.com',
    'itunes.apple.com',
    'acdn.adnxs.com',
    'adm.shinobi.jp',
    'ib-game.jp',
    'fruitmail.net',
    'pointservice.com',
    'gpoint.co.jp',
    'pointtown.com',
    'point-ex.jp',
    'pointi.jp',
    'nppoint.jp',
    'moppy.jp',
    'hapitas.jp',
    'osaifu.com',
    'gendama.jp',
    'warau.jp',
    'nyandaful.jp',
    'openx.net',
    'googlesyndication.com',
    'socdm.com'
])

EXCLUDE_DOMAIN_PREFIXES = [
    'fruitmail',
    'pointtown',
    'moppy',
    'chobirich',
    'osaifu',
    'warau',
    'potora',
    'osaifu-sp'
]

EXCLUDE_BASE_DOMAINS = [
    'ib-game.jp',
    'fruitmail.net',
    'pointservice.com',
    'gpoint.co.jp',
    'pointtown.com',
    'point-ex.jp',
    'pointi.jp',
    'nppoint.jp',
    'moppy.jp',
    'hapitas.jp',
    'osaifu.com',
    'gendama.jp',
    'warau.jp',
    'nyandaful.jp',
    'openx.net',
    'googlesyndication.com',
    'socdm.com'
]

EXCLUDE_ENDS_WITH_DOMAINS = [
]

def domain_normalize_from_tp(url):
    domain = get_domain(url)
    return domain_normalize(domain)

class TestSSUdf(unittest.TestCase):

    def test_get_os_type(self):
        self.assertEqual(get_os_type('iPhone OS 9', ''), 'iOS')
        self.assertEqual(get_os_type('iPod', ''), 'iOS')
        self.assertEqual(get_os_type('iPad', ''), 'iOS')
        self.assertEqual(get_os_type('Android 4.0', ''), 'Android')
        self.assertEqual(get_os_type('Linux', ''), 'Linux')
        self.assertEqual(get_os_type('Mac OS X', ''), 'Mac')
        self.assertEqual(get_os_type('Windows 8', ''), 'Windows')
        self.assertEqual(get_os_type('Windows Phone 7', ''), 'Windows')
        self.assertEqual(get_os_type('something', ''), 'Unknown')
        self.assertEqual(get_os_type('something', 'Mozilla/5.0 (PlayStation 4 6.71) AppleWebKit/605.1.15 (KHTML, like Gecko)'), 'PlayStation')
        self.assertEqual(get_os_type('Unknown', 'Mozilla/5.0 (Nintendo WiiU) AppleWebKit/536.30 (KHTML, like Gecko) NX/3.0.4.2.13 NintendoBrowser/4.3.2.11274.JP'), 'Nintendo')

    def test_get_device_from_os(self):
        self.assertEqual(get_device_from_os('iPhone'), 'sp')
        self.assertEqual(get_device_from_os('Android 4.0'), 'sp')
        self.assertEqual(get_device_from_os('iPad'), 'sp')
        self.assertEqual(get_device_from_os('iPod'), 'sp')
        self.assertEqual(get_device_from_os('Mac OS X'), 'pc')
        self.assertEqual(get_device_from_os('Linux'), 'pc')
        self.assertEqual(get_device_from_os('Windows 8'), 'pc')
        self.assertEqual(get_device_from_os('Windows Phone 7'), 'pc')
        self.assertEqual(get_device_from_os('something'), 'pc')
        self.assertEqual(get_device_from_os('Unknown'), 'pc')

    def test_get_device_from_os_mediatype(self):
        self.assertEqual(get_device_from_os_mediatype('Unknown', 'app'), 'app')
        self.assertEqual(get_device_from_os_mediatype('Unknown', 'site'), 'pc')
        self.assertEqual(get_device_from_os_mediatype('iPad', 'app'), 'app')
        self.assertEqual(get_device_from_os_mediatype('iPad', 'site'), 'sp')
        self.assertEqual(get_device_from_os_mediatype('Mac OS X', 'app'), 'app')
        self.assertEqual(get_device_from_os_mediatype('Mac OS X', 'site'), 'pc')

    def test_timestamp_to_hour(self):
        self.assertEqual(timestamp_to_hour(1547082000), 1)
        self.assertEqual(timestamp_to_hour(1547114400), 10)
        self.assertEqual(timestamp_to_hour(1547132400), 15)
        self.assertEqual(timestamp_to_hour(1547161200), 23)

    def test_get_domain_or_app(self):
        self.assertEqual(get_domain_or_app({'media_type': 'app', 'tp': 'http://yahoo.co.jp', 'app_bundle_id': 'app.bundle_id.123'}), 'app.bundle_id.123')
        self.assertEqual(get_domain_or_app({'media_type': 'app', 'tp': 'http://yahoo.co.jp', 'app_track_id': '830340223'}), '830340223')
        self.assertEqual(get_domain_or_app({'media_type': 'app', 'tp': 'http://yahoo.co.jp'}), None)
        self.assertEqual(get_domain_or_app({'media_type': 'site', 'tp': 'http://yahoo.co.jp/aaaa/bbbb', 'app_track_id': '830340223'}), 'yahoo.co.jp')
        self.assertEqual(get_domain_or_app({'media_type': 'site', 'tp': 'http://blog.livedoor.jp/aaaa/bbbb', 'app_track_id': '830340223'}), 'blog.livedoor.jp/aaaa')

    def test_get_domain_or_app_from_tp(self):
        self.assertEqual(get_domain_or_app_from_tp('app', 'http://yahoo.co.jp', 'app.bundle_id.123'), 'app.bundle_id.123')
        self.assertEqual(get_domain_or_app_from_tp('site', 'http://yahoo.co.jp', 'app.bundle_id.123'), 'yahoo.co.jp')
        self.assertEqual(get_domain_or_app_from_tp('site', 'http://d.hatena.ne.jp/foo/bar', 'app.bundle_id.123'), 'd.hatena.ne.jp/foo')

    def test_get_domain_or_app_from_domain(self):
        self.assertEqual(get_domain_or_app_from_domain('app', 'yahoo.co.jp', 'app.bundle_id.123'), 'app.bundle_id.123')
        self.assertEqual(get_domain_or_app_from_domain('site', 'yahoo.co.jp', 'app.bundle_id.123'), 'yahoo.co.jp')

    def test_exclude_domain_prefix(self):
        self.assertEqual(exclude_domain_prefix('www.yahoo.co.jp'), 'yahoo.co.jp')
        self.assertEqual(exclude_domain_prefix('sp.example.com'), 'example.com')
        self.assertEqual(exclude_domain_prefix('touch.pixiv.com'), 'pixiv.com')

    def test_get_birth(self):
        self.assertEqual(get_birth('WqurZMCo5kgAAHjyXWwAAAAA'), 1521199972)
        self.assertEqual(get_birth('WXcFBcCo4VUAABoGdUMAAAAA'), 1500972293)
        self.assertEqual(get_birth('anid:9fb7f464-8217-47f0-b880-6dc0b90f4fb7'), None)
        self.assertEqual(get_birth('WE4dncCo4VEAAH2iXwYAAAAA'), 1481514397)
        self.assertEqual(get_birth('XQMZ6sCo8WgAAVbDwW8AAAAA'), 1560484330)
        self.assertEqual(get_birth('anid:14e89137-6edc-47e0-b23d-7d630516c377'), None)

    def test_is_pmp(self):
        self.assertEqual(is_pmp({'deal_ver2':''}), 0)
        self.assertEqual(is_pmp({'deal_ver2':'2379-062196,2379-062305,2379-065420,2379-065509,2379-065596,2379-065686'}), 1)
        self.assertEqual(is_pmp({'pmp.deal_ids':''}), 0)
        self.assertEqual(is_pmp({'pmp.deal_ids':'[]'}), 0)
        self.assertEqual(is_pmp({'pmp.deal_ids':'["37ab8.1bd50.57c3"]'}), 1)

    def test_is_movie(self):
        self.assertEqual(is_movie({'proto':''}), 0)
        self.assertEqual(is_movie({'proto':'teads'}), 1)
        self.assertEqual(is_movie({'proto':'appvador'}), 1)
        self.assertEqual(is_movie({'proto':'something'}), 0)

    def test_get_int_or_zero(self):
        self.assertEqual(get_int_or_zero('10'), 10)
        self.assertEqual(get_int_or_zero(''), 0)
        self.assertEqual(get_int_or_zero(None), 0)
        self.assertEqual(get_int_or_zero('foo'), 0)

    def test_get_int_or_default(self):
        self.assertEqual(get_int_or_default('10'), 10)
        self.assertEqual(get_int_or_default('10', 0), 10)
        self.assertEqual(get_int_or_default(''), -1)
        self.assertEqual(get_int_or_default('', 0), 0)
        self.assertEqual(get_int_or_default(None), -1)
        self.assertEqual(get_int_or_default('foo'), -1)
        self.assertEqual(get_int_or_default('foo', 0), 0)

    def test_get_domain(self):
        self.assertEqual(get_domain('http://www.nikkan-gendai.com/foo/bar/baz'), 'www.nikkan-gendai.com/foo/bar/baz')
        self.assertEqual(get_domain('http://www.nikkan-gendai.com/foo/bar'), 'www.nikkan-gendai.com/foo/bar')
        self.assertEqual(get_domain('http://www.nikkan-gendai.com/foo'), 'www.nikkan-gendai.com/foo')
        self.assertEqual(get_domain('http://www.nikkan-gendai.com/'), 'www.nikkan-gendai.com')
        self.assertEqual(get_domain('http://d.hatena.ne.jp/foo'), 'd.hatena.ne.jp/foo')
        self.assertEqual(get_domain('http://d.hatena.ne.jp/foo/bar'), 'd.hatena.ne.jp/foo')
        self.assertEqual(get_domain('http://blog.livedoor.com/'), 'blog.livedoor.com')
        self.assertEqual(get_domain('http://blog.livedoor.com/foobar'), 'blog.livedoor.com/foobar')
        self.assertEqual(get_domain('http://blog.livedoor.com/FooBar'), 'blog.livedoor.com/foobar')
        self.assertEqual(get_domain('http://www.zakzak.co.jp/?ownedref=feature_not%20set_crumb'), 'www.zakzak.co.jp')
        self.assertEqual(get_domain('http://local.ameblo.jp:3000/risa-celeblog/entry-12490647907.html'), 'local.ameblo.jp')
        self.assertEqual(get_domain('https://imeditor:iGWerSmH@koimemo.com/search'), 'koimemo.com')
        self.assertEqual(get_domain('https://www.nikkan-gendai.com/://'), 'www.nikkan-gendai.com/:')

    def test_domain_normalize(self):
        self.assertEqual(domain_normalize('www.nikkan-gendai.com/foo/bar/baz'), 'nikkan-gendai.com/foo/bar/baz')
        self.assertEqual(domain_normalize('www.yahoo.co.jp/foo'), 'yahoo.co.jp/foo')
        self.assertEqual(domain_normalize('socdm.com'), None)

    def test_domain_normalize_from_tp(self):
        self.assertEqual(domain_normalize_from_tp('http://www.nikkan-gendai.com/foo/bar/baz'), 'nikkan-gendai.com/foo/bar/baz')
        self.assertEqual(domain_normalize_from_tp('http://www.yahoo.co.jp/foo/bar/baz'), 'yahoo.co.jp')
        self.assertEqual(domain_normalize_from_tp('http://socdm.com/foo/bar/baz'), None)
        self.assertEqual(domain_normalize_from_tp('http://prefix.socdm.com/foo/bar/baz'), None)
        self.assertEqual(domain_normalize_from_tp('http://tpc.googlesyndication.com/foo/bar/baz'), None)
        self.assertEqual(domain_normalize_from_tp('http://osaifu.example.com/foo/bar/baz'), None)

    def test_get_ssp_page_id(self):
        self.assertEqual(get_ssp_page_id(32605), 8338)
        self.assertEqual(get_ssp_page_id(9918), 8392)
        self.assertEqual(get_ssp_page_id(8392), 8392)
        self.assertEqual(get_ssp_page_id('hoge'), -1)
        self.assertEqual(get_ssp_page_id(23109), 23109)
        self.assertEqual(get_ssp_page_id(10348), 8107)
        self.assertEqual(get_ssp_page_id(32607), 8107)

if __name__ == '__main__':
    # /opt/pyspark-python_current/bin/python ss_common.py
    unittest.main()
