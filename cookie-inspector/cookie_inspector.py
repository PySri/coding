#!/usr/bin/env python3
"""
cookie_inspector.py — scan a folder of browser cookie stores and describe every cookie.

Supported inputs (auto-detected, folder is searched recursively):
  * Chromium browsers (Chrome, Edge, Brave, Opera, Vivaldi)  -> SQLite file "Cookies"
  * Firefox                                                   -> SQLite file "cookies.sqlite"
  * Safari                                                    -> "Cookies.binarycookies"
  * Netscape / curl / wget format                             -> "cookies.txt"
  * JSON exports (EditThisCookie, Cookie-Editor, Playwright/Puppeteer storage state)
  * Legacy Windows INetCookies / IE .txt cookie files

For each cookie it reports: owner/vendor, purpose category, plain-English description,
domain, path, first/third-party hint, created / expires / last-accessed times, lifetime,
Secure / HttpOnly / SameSite flags, value size and shape, and any security/privacy findings.

Cookie VALUES are not printed by default (they are often live login tokens).
Use --show-values to include a short masked preview. Encrypted Chromium values are
never decrypted.

Usage:
  python cookie_inspector.py PATH [-o report.html] [--csv cookies.csv] [--json cookies.json]
                                  [--show-values] [--site example.com]
  python cookie_inspector.py --locations     # print where browsers keep cookies on this OS

Deleting cookies by category (dry run unless --apply is given):
  python cookie_inspector.py PATH --delete Advertising,Analytics            # preview only
  python cookie_inspector.py PATH --delete Advertising,Analytics --apply    # really delete
  python cookie_inspector.py PATH --delete-expired --apply                  # expired cookies only
  python cookie_inspector.py PATH --delete Advertising --keep-domain google.com --apply

Deleting specific cookies:
  python cookie_inspector.py PATH --delete-id 3f9a1c2e --apply              # one exact cookie (ID from report)
  python cookie_inspector.py PATH --delete Functional --delete-name lang    # one named cookie in a category
  python cookie_inspector.py PATH --delete-domain example.com --apply      # every cookie for a domain
  python cookie_inspector.py PATH --delete Analytics --delete-domain example.com --apply
  python cookie_inspector.py PATH --delete-name "_ga*" --apply             # wildcards allowed

  All selectors combine with AND (each one narrows the selection); values given to the
  same option combine with OR (e.g. --delete Ads,Analytics or two --delete-domain options).

  Categories: "Strictly necessary", Authentication, Security, Functional, Consent,
              Analytics, Advertising, Unknown, or "all" (aliases: ads, marketing,
              tracking, stats, auth, login, necessary, essential, preferences).
  * Close the browser completely first; a running browser locks or rewrites its cookie file.
  * A timestamped backup of every changed file is written next to it (skip with --no-backup).
  * Supported for Chromium browsers, Firefox, cookies.txt, JSON exports and IE files.
    Safari's binary cookie file is read-only here.

Only standard-library modules are used (Python 3.8+).
"""

import argparse
import base64
import hashlib
import csv
import datetime as dt
import fnmatch
import html
import json
import os
import platform
import re
import shutil
import sqlite3
import struct
import sys
import tempfile
from collections import Counter, defaultdict

UTC = dt.timezone.utc
NOW = dt.datetime.now(UTC)

# ---------------------------------------------------------------------------
# Knowledge base: well-known cookie names
# (pattern, vendor, category, description)   patterns use shell wildcards
# ---------------------------------------------------------------------------
KNOWN_COOKIES = [
    # Google Analytics / Google
    ("_ga", "Google Analytics", "Analytics", "Distinguishes unique visitors; usually lasts 2 years."),
    ("_ga_*", "Google Analytics 4", "Analytics", "Keeps GA4 session state for a specific property."),
    ("_gid", "Google Analytics", "Analytics", "Distinguishes visitors over a 24-hour window."),
    ("_gat*", "Google Analytics", "Analytics", "Throttles the request rate to Google Analytics."),
    ("__utm*", "Google Analytics (legacy)", "Analytics", "Legacy Urchin/Universal Analytics visitor and session tracking."),
    ("_gcl_*", "Google Ads", "Advertising", "Stores ad-click information to attribute conversions."),
    ("IDE", "Google DoubleClick", "Advertising", "Records ad interactions to measure and target ads across sites."),
    ("test_cookie", "Google DoubleClick", "Advertising", "Checks whether the browser accepts cookies."),
    ("DSID", "Google DoubleClick", "Advertising", "Links ad activity across devices for signed-in users."),
    ("NID", "Google", "Functional", "Stores Google preferences (language, results count) and ad personalisation data."),
    ("AEC", "Google", "Security", "Ensures requests in a browsing session come from the user, not other sites."),
    ("1P_JAR", "Google", "Advertising", "Collects site statistics and tracks conversion rates."),
    ("ANID", "Google", "Advertising", "Stores ad personalisation settings."),
    ("CONSENT", "Google", "Consent", "Stores the user's cookie-consent choices for Google services."),
    ("SOCS", "Google", "Consent", "Stores the user's cookie-consent choices for Google services."),
    ("SID", "Google", "Authentication", "Part of Google sign-in; identifies the signed-in account."),
    ("HSID", "Google", "Authentication", "Google sign-in security cookie (HTTP-only half of SID)."),
    ("SSID", "Google", "Authentication", "Google sign-in security cookie (secure half of SID)."),
    ("APISID", "Google", "Authentication", "Google sign-in cookie used by embedded Google services."),
    ("SAPISID", "Google", "Authentication", "Google sign-in cookie used by embedded Google services."),
    ("__Secure-*PSID*", "Google", "Authentication", "Google sign-in / ad-personalisation identifier (partitioned or third-party variant)."),
    ("__Secure-*PAPISID", "Google", "Authentication", "Google sign-in cookie (secure variant)."),
    ("__Secure-ENID", "Google", "Functional", "Remembers Google preferences."),
    ("SIDCC", "Google", "Security", "Protects Google account data from unauthorised access."),
    ("__Secure-*PSIDCC", "Google", "Security", "Protects Google account data from unauthorised access."),
    ("__Secure-*PSIDTS", "Google", "Security", "Google sign-in session timestamp."),
    ("OTZ", "Google", "Analytics", "Aggregate Google site analytics."),
    ("YSC", "YouTube", "Analytics", "Counts views of embedded YouTube videos for this session."),
    ("VISITOR_INFO1_LIVE", "YouTube", "Advertising", "Estimates bandwidth and tracks preferences of viewers of embedded videos."),
    ("VISITOR_PRIVACY_METADATA", "YouTube", "Consent", "Stores the user's consent state for YouTube."),
    ("PREF", "YouTube", "Functional", "Stores YouTube player preferences."),
    ("LOGIN_INFO", "YouTube", "Authentication", "YouTube sign-in information."),
    # Meta
    ("_fbp", "Meta (Facebook)", "Advertising", "Facebook Pixel browser ID used to deliver and measure ads."),
    ("_fbc", "Meta (Facebook)", "Advertising", "Stores the last Facebook ad click for attribution."),
    ("fr", "Meta (Facebook)", "Advertising", "Facebook ad delivery and measurement identifier."),
    ("datr", "Meta (Facebook)", "Security", "Identifies the browser for Facebook security and integrity."),
    ("c_user", "Meta (Facebook)", "Authentication", "Facebook user ID of the signed-in account."),
    ("xs", "Meta (Facebook)", "Authentication", "Facebook session token (paired with c_user)."),
    ("sb", "Meta (Facebook)", "Security", "Facebook browser identification for login security."),
    # Microsoft
    ("MUID", "Microsoft", "Advertising", "Microsoft-wide unique user ID used for ads and analytics."),
    ("MUIDB", "Microsoft", "Advertising", "Microsoft user ID (backup)."),
    ("_uetsid", "Microsoft Bing Ads", "Advertising", "Bing UET session identifier for conversion tracking."),
    ("_uetvid", "Microsoft Bing Ads", "Advertising", "Bing UET visitor identifier for conversion tracking."),
    ("_clck", "Microsoft Clarity", "Analytics", "Clarity user ID and preferences."),
    ("_clsk", "Microsoft Clarity", "Analytics", "Joins page views into a single Clarity session recording."),
    ("CLID", "Microsoft Clarity", "Analytics", "Identifies the first time Clarity saw this user."),
    ("SM", "Microsoft Clarity", "Analytics", "Synchronises the MUID across Microsoft domains."),
    ("ANONCHK", "Microsoft", "Analytics", "Indicates whether MUID is transferred to ANID."),
    ("ESTSAUTH*", "Microsoft Entra ID", "Authentication", "Microsoft account / Entra sign-in session."),
    # LinkedIn
    ("bcookie", "LinkedIn", "Advertising", "LinkedIn browser identifier."),
    ("bscookie", "LinkedIn", "Security", "LinkedIn secure browser identifier."),
    ("lidc", "LinkedIn", "Functional", "LinkedIn data-centre routing."),
    ("li_gc", "LinkedIn", "Consent", "Stores consent for non-essential LinkedIn cookies."),
    ("li_at", "LinkedIn", "Authentication", "LinkedIn session token."),
    ("li_sugr", "LinkedIn", "Advertising", "Probabilistic identity matching for LinkedIn ads."),
    ("UserMatchHistory", "LinkedIn", "Advertising", "LinkedIn Ads ID syncing."),
    ("AnalyticsSyncHistory", "LinkedIn", "Analytics", "Records when the user was last synced with LinkedIn analytics."),
    ("lang", "(generic)", "Functional", "Remembers the selected language."),
    # X / Twitter, TikTok, Pinterest, Reddit, Snap
    ("personalization_id", "X (Twitter)", "Advertising", "Tracks activity for X/Twitter ad personalisation."),
    ("guest_id*", "X (Twitter)", "Advertising", "Identifies a logged-out X/Twitter visitor."),
    ("auth_token", "X (Twitter) / various", "Authentication", "Session authentication token."),
    ("ct0", "X (Twitter)", "Security", "X/Twitter CSRF protection token."),
    ("_ttp", "TikTok", "Advertising", "TikTok Pixel identifier for ad measurement."),
    ("ttwid", "TikTok", "Analytics", "TikTok visitor identifier."),
    ("_pin_unauth", "Pinterest", "Advertising", "Pinterest tag identifier for logged-out users."),
    ("_pinterest_*", "Pinterest", "Advertising", "Pinterest session / ad tracking."),
    ("_rdt_uuid", "Reddit", "Advertising", "Reddit Pixel identifier for ad measurement."),
    ("_scid*", "Snapchat", "Advertising", "Snap Pixel identifier for ad measurement."),
    # Analytics / marketing platforms
    ("_hjSession*", "Hotjar", "Analytics", "Holds current Hotjar session data."),
    ("_hjid", "Hotjar", "Analytics", "Hotjar visitor ID."),
    ("_hj*", "Hotjar", "Analytics", "Hotjar behaviour analytics / recordings."),
    ("__hstc", "HubSpot", "Analytics", "HubSpot visitor tracking (first/last visit, session count)."),
    ("__hssc", "HubSpot", "Analytics", "HubSpot session tracking."),
    ("__hssrc", "HubSpot", "Analytics", "Detects whether the browser restarted (new HubSpot session)."),
    ("hubspotutk", "HubSpot", "Analytics", "HubSpot visitor identity, passed on form submission."),
    ("ajs_anonymous_id", "Segment", "Analytics", "Segment anonymous visitor ID."),
    ("ajs_user_id", "Segment", "Analytics", "Segment identified user ID."),
    ("mp_*_mixpanel", "Mixpanel", "Analytics", "Mixpanel visitor and super-property data."),
    ("amp_*", "Amplitude", "Analytics", "Amplitude device and session identifiers."),
    ("AMP_*", "Amplitude", "Analytics", "Amplitude device and session identifiers."),
    ("_pk_id*", "Matomo", "Analytics", "Matomo visitor ID."),
    ("_pk_ses*", "Matomo", "Analytics", "Matomo short-lived session cookie."),
    ("s_cc", "Adobe Analytics", "Analytics", "Checks whether cookies are enabled for Adobe Analytics."),
    ("s_sq", "Adobe Analytics", "Analytics", "Stores the previous link click for Adobe ClickMap."),
    ("s_vi", "Adobe Analytics", "Analytics", "Adobe Analytics visitor ID."),
    ("AMCV_*", "Adobe Experience Cloud", "Analytics", "Adobe Experience Cloud visitor ID."),
    ("AMCVS_*", "Adobe Experience Cloud", "Analytics", "Adobe session flag."),
    ("demdex", "Adobe Audience Manager", "Advertising", "Adobe Audience Manager visitor ID."),
    ("intercom-*", "Intercom", "Functional", "Intercom chat widget session / visitor ID."),
    ("__stripe_mid", "Stripe", "Security", "Stripe fraud prevention (device ID)."),
    ("__stripe_sid", "Stripe", "Security", "Stripe fraud prevention (session ID)."),
    ("_shopify_y", "Shopify", "Analytics", "Shopify visitor analytics."),
    ("_shopify_s", "Shopify", "Analytics", "Shopify session analytics."),
    ("_shopify_sa_*", "Shopify", "Analytics", "Shopify marketing attribution."),
    ("cart", "Shopify / e-commerce", "Strictly necessary", "Stores the shopping cart."),
    ("_secure_session_id", "Shopify", "Strictly necessary", "Shopify storefront session."),
    # GitHub / Apple
    ("_octo", "GitHub", "Analytics", "GitHub visitor identifier for analytics."),
    ("user_session", "GitHub", "Authentication", "GitHub signed-in session."),
    ("__Host-user_session_same_site", "GitHub", "Authentication", "GitHub session (SameSite-strict copy)."),
    ("logged_in", "GitHub", "Functional", "Flags whether the user is signed in to GitHub."),
    ("_gh_sess", "GitHub", "Strictly necessary", "GitHub temporary session state."),
    ("dssid2", "Apple", "Analytics", "Apple store / site session identifier."),
    ("geo", "Apple / various", "Functional", "Stores the visitor's country for localisation."),
    # Ad tech
    ("uuid2", "Xandr (Microsoft)", "Advertising", "Real-time bidding user ID."),
    ("anj", "Xandr (Microsoft)", "Advertising", "Real-time bidding user data."),
    ("TDID", "The Trade Desk", "Advertising", "Trade Desk user ID for ad targeting."),
    ("TDCPM", "The Trade Desk", "Advertising", "Trade Desk ad-buying data."),
    ("uid", "Criteo / various", "Advertising", "Ad-network user identifier."),
    ("cto_*", "Criteo", "Advertising", "Criteo retargeting identifiers."),
    ("ad-id", "Amazon Ads", "Advertising", "Amazon advertising identifier."),
    ("ad-privacy", "Amazon Ads", "Consent", "Amazon ad opt-out preference."),
    ("A3", "Yahoo", "Advertising", "Yahoo ad targeting identifier."),
    ("KTPCACOOKIE", "Index Exchange", "Advertising", "Checks third-party cookie support."),
    ("CMID", "Index Exchange", "Advertising", "Ad-exchange user ID."),
    ("t_gid", "Taboola", "Advertising", "Taboola user ID for content recommendations."),
    ("obuid", "Outbrain", "Advertising", "Outbrain user ID."),
    ("UID", "Comscore", "Analytics", "Comscore audience measurement."),
    ("XANDR_PANID", "Xandr (Microsoft)", "Advertising", "Privacy-preserving advertising ID."),
    # Consent management
    ("OptanonConsent", "OneTrust", "Consent", "Stores which cookie categories the user accepted."),
    ("OptanonAlertBoxClosed", "OneTrust", "Consent", "Records when the consent banner was dismissed."),
    ("eupubconsent-v2", "IAB TCF", "Consent", "IAB Transparency & Consent Framework string."),
    ("euconsent-v2", "IAB TCF", "Consent", "IAB Transparency & Consent Framework string."),
    ("CookieConsent", "Cookiebot", "Consent", "Stores the user's consent state."),
    ("cookieyes-consent", "CookieYes", "Consent", "Stores the user's consent state."),
    ("cmplz_*", "Complianz", "Consent", "Stores the user's consent state."),
    ("didomi_token", "Didomi", "Consent", "Stores the user's consent state."),
    ("usprivacy", "IAB CCPA", "Consent", "US Privacy (CCPA) opt-out string."),
    ("_iub_cs-*", "iubenda", "Consent", "Stores the user's consent state."),
    # Infrastructure / security
    ("__cf_bm", "Cloudflare", "Security", "Cloudflare Bot Management; distinguishes humans from bots (30 min)."),
    ("cf_clearance", "Cloudflare", "Security", "Proves the browser passed a Cloudflare challenge."),
    ("__cfruid", "Cloudflare", "Security", "Cloudflare rate-limiting: identifies trusted traffic."),
    ("_cfuvid", "Cloudflare", "Security", "Cloudflare rate-limiting for visitors sharing an IP."),
    ("__cflb", "Cloudflare", "Strictly necessary", "Cloudflare load-balancer session affinity."),
    ("AWSALB", "AWS", "Strictly necessary", "AWS Application Load Balancer sticky session."),
    ("AWSALBCORS", "AWS", "Strictly necessary", "AWS load balancer sticky session (cross-site variant)."),
    ("AWSELB", "AWS", "Strictly necessary", "AWS Classic Load Balancer sticky session."),
    ("datadome", "DataDome", "Security", "DataDome bot protection."),
    ("_abck", "Akamai", "Security", "Akamai Bot Manager."),
    ("bm_*", "Akamai", "Security", "Akamai Bot Manager."),
    ("ak_bmsc", "Akamai", "Security", "Akamai Bot Manager."),
    ("incap_ses_*", "Imperva", "Security", "Imperva Incapsula session tracking for bot protection."),
    ("visid_incap_*", "Imperva", "Security", "Imperva Incapsula visitor ID."),
    ("__ddg*", "DDoS-Guard", "Security", "DDoS-Guard bot protection."),
    # Frameworks / generic session
    ("PHPSESSID", "PHP", "Strictly necessary", "PHP server-side session ID."),
    ("JSESSIONID", "Java (Servlet)", "Strictly necessary", "Java server-side session ID."),
    ("ASP.NET_SessionId", "ASP.NET", "Strictly necessary", "ASP.NET server-side session ID."),
    (".AspNetCore.*", "ASP.NET Core", "Strictly necessary", "ASP.NET Core session / auth / antiforgery cookie."),
    ("__RequestVerificationToken", "ASP.NET", "Security", "ASP.NET anti-forgery (CSRF) token."),
    ("ARRAffinity*", "Azure App Service", "Strictly necessary", "Azure load-balancer sticky session."),
    ("connect.sid", "Express.js", "Strictly necessary", "Node/Express session ID."),
    ("laravel_session", "Laravel", "Strictly necessary", "Laravel session ID."),
    ("XSRF-TOKEN", "Laravel / Angular", "Security", "CSRF protection token."),
    ("csrftoken", "Django", "Security", "Django CSRF protection token."),
    ("sessionid", "Django / various", "Strictly necessary", "Server-side session ID."),
    ("_session_id", "Ruby on Rails / various", "Strictly necessary", "Server-side session ID."),
    ("__Host-next-auth.*", "NextAuth.js", "Authentication", "NextAuth.js session / CSRF cookie."),
    ("next-auth.*", "NextAuth.js", "Authentication", "NextAuth.js session / CSRF / callback cookie."),
    ("__Secure-next-auth.*", "NextAuth.js", "Authentication", "NextAuth.js session cookie."),
    ("wordpress_logged_in_*", "WordPress", "Authentication", "WordPress signed-in user."),
    ("wordpress_sec_*", "WordPress", "Authentication", "WordPress admin authentication."),
    ("wordpress_test_cookie", "WordPress", "Strictly necessary", "Checks whether cookies are enabled."),
    ("wp-settings-*", "WordPress", "Functional", "WordPress admin UI preferences."),
    ("wp-settings-time-*", "WordPress", "Functional", "Time the WordPress settings were saved."),
    ("SSESS*", "Drupal", "Strictly necessary", "Drupal secure session."),
    ("SESS*", "Drupal", "Strictly necessary", "Drupal session."),
    ("__Host-*", "(site-defined)", "Security", "'__Host-' prefix: browser enforces Secure, no Domain, Path=/."),
    ("__Secure-*", "(site-defined)", "Security", "'__Secure-' prefix: browser enforces Secure flag."),
]

TRACKER_DOMAINS = {
    "doubleclick.net": "Google DoubleClick", "google-analytics.com": "Google Analytics",
    "googleadservices.com": "Google Ads", "googlesyndication.com": "Google AdSense",
    "facebook.com": "Meta", "facebook.net": "Meta", "instagram.com": "Meta",
    "linkedin.com": "LinkedIn", "ads.linkedin.com": "LinkedIn Ads",
    "twitter.com": "X (Twitter)", "x.com": "X (Twitter)", "ads-twitter.com": "X Ads",
    "tiktok.com": "TikTok", "bing.com": "Microsoft Bing", "clarity.ms": "Microsoft Clarity",
    "hotjar.com": "Hotjar", "criteo.com": "Criteo", "criteo.net": "Criteo",
    "adnxs.com": "Xandr", "rubiconproject.com": "Magnite", "pubmatic.com": "PubMatic",
    "taboola.com": "Taboola", "outbrain.com": "Outbrain", "scorecardresearch.com": "Comscore",
    "quantserve.com": "Quantcast", "demdex.net": "Adobe Audience Manager",
    "everesttech.net": "Adobe Advertising", "adsrvr.org": "The Trade Desk",
    "amazon-adsystem.com": "Amazon Ads", "casalemedia.com": "Index Exchange",
    "openx.net": "OpenX", "bidswitch.net": "BidSwitch", "rlcdn.com": "LiveRamp",
    "agkn.com": "Neustar", "mathtag.com": "MediaMath", "crwdcntrl.net": "Lotame",
    "tapad.com": "Tapad", "pinterest.com": "Pinterest", "reddit.com": "Reddit",
    "snapchat.com": "Snap", "yahoo.com": "Yahoo", "3lift.com": "TripleLift",
    "sharethrough.com": "Sharethrough", "smartadserver.com": "Equativ",
    "teads.tv": "Teads", "id5-sync.com": "ID5", "hubspot.com": "HubSpot",
    "hs-analytics.net": "HubSpot", "mixpanel.com": "Mixpanel", "segment.io": "Segment",
}

CATEGORY_ORDER = ["Strictly necessary", "Authentication", "Security", "Functional",
                  "Consent", "Analytics", "Advertising", "Unknown"]

HEURISTICS = [  # (regex on lowercase name, category, description)
    (r"csrf|xsrf|antiforgery", "Security", "Looks like a CSRF / anti-forgery token."),
    (r"consent|gdpr|ccpa|cookie_?notice|cookielaw|cookie_?banner", "Consent", "Looks like a cookie-consent preference."),
    (r"(^|_)(auth|token|jwt|access|refresh|remember|login|logged)", "Authentication", "Looks like an authentication / login token."),
    (r"sess|sid$|^sid", "Strictly necessary", "Looks like a session identifier."),
    (r"cart|basket|checkout", "Strictly necessary", "Looks like shopping-cart state."),
    (r"lang|locale|currency|country|region|theme|timezone|tz$|pref|settings|dark", "Functional", "Looks like a user preference (language, theme, region...)."),
    (r"(^|_)(utm|campaign|ref|referr|source|gclid|fbclid|msclkid)", "Analytics", "Looks like campaign / referral attribution."),
    (r"visitor|visit|uid|uuid|_id$|^id$|track|analytics|stats|ab_?test|experiment|variant", "Analytics", "Looks like a visitor / experiment identifier."),
    (r"(^|_)(ad|ads|adv|pixel|retarget|segment|audience)", "Advertising", "Looks like an advertising identifier."),
]

# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------
EPOCH_1601 = dt.datetime(1601, 1, 1, tzinfo=UTC)
EPOCH_2001 = dt.datetime(2001, 1, 1, tzinfo=UTC)


def from_unix(seconds):
    try:
        if seconds is None or seconds <= 0:
            return None
        return dt.datetime.fromtimestamp(float(seconds), UTC)
    except (OverflowError, OSError, ValueError):
        return dt.datetime.max.replace(tzinfo=UTC)


def from_webkit(micro):  # Chromium: microseconds since 1601-01-01
    if not micro:
        return None
    try:
        return EPOCH_1601 + dt.timedelta(microseconds=int(micro))
    except OverflowError:
        return dt.datetime.max.replace(tzinfo=UTC)


def from_filetime(ft):  # Windows FILETIME: 100 ns since 1601-01-01
    if not ft:
        return None
    try:
        return EPOCH_1601 + dt.timedelta(microseconds=int(ft) // 10)
    except OverflowError:
        return dt.datetime.max.replace(tzinfo=UTC)


def from_mac(seconds):  # Safari: seconds since 2001-01-01
    if not seconds:
        return None
    try:
        return EPOCH_2001 + dt.timedelta(seconds=float(seconds))
    except OverflowError:
        return dt.datetime.max.replace(tzinfo=UTC)


def fmt_time(t):
    if t is None:
        return ""
    if t.year >= 9999:
        return "never (max date)"
    return t.astimezone().strftime("%Y-%m-%d %H:%M %Z")


def human_duration(delta):
    if delta is None:
        return ""
    secs = delta.total_seconds()
    sign = "-" if secs < 0 else ""
    secs = abs(secs)
    for unit, size in (("year", 31557600), ("day", 86400), ("hour", 3600), ("minute", 60)):
        if secs >= size:
            n = secs / size
            return f"{sign}{n:.1f} {unit}s" if unit == "year" else f"{sign}{int(n)} {unit}{'s' if int(n) != 1 else ''}"
    return f"{sign}{int(secs)} seconds"


# ---------------------------------------------------------------------------
# Cookie record
# ---------------------------------------------------------------------------
SAMESITE_CHROMIUM = {-1: "Unspecified (defaults to Lax)", 0: "None", 1: "Lax", 2: "Strict"}
SAMESITE_FIREFOX = {0: "None", 1: "Lax", 2: "Strict", 256: "Unspecified (defaults to Lax)"}


def new_cookie(**kw):
    base = dict(browser="", source_file="", name="", value=None, value_encrypted=False,
                value_length=0, domain="", path="/", created=None, expires=None,
                last_accessed=None, secure=None, http_only=None, same_site="",
                session=False, host_only=None, partition_key="",
                _kind="", _rowid=None, _index=None, deletion="")
    base.update(kw)
    if base["value"] is not None and not base["value_length"]:
        base["value_length"] = len(base["value"])
    return base


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------
def _open_sqlite_copy(path):
    """Copy the DB (+WAL/SHM) to a temp dir so a running browser's lock doesn't block us."""
    tmp = tempfile.mkdtemp(prefix="cookieinspect_")
    dst = os.path.join(tmp, os.path.basename(path))
    shutil.copy2(path, dst)
    for ext in ("-wal", "-shm", "-journal"):
        if os.path.exists(path + ext):
            shutil.copy2(path + ext, dst + ext)
    con = sqlite3.connect(dst)
    con.row_factory = sqlite3.Row
    return con, tmp


def _columns(con, table):
    return {r[1] for r in con.execute(f"PRAGMA table_info({table})")}


def guess_chromium_browser(path):
    p = path.lower()
    for key, name in (("edge", "Microsoft Edge"), ("brave", "Brave"), ("opera", "Opera"),
                      ("vivaldi", "Vivaldi"), ("chromium", "Chromium"), ("chrome", "Google Chrome")):
        if key in p:
            return name
    return "Chromium-based browser"


def parse_sqlite(path, show_values):
    con, tmp = _open_sqlite_copy(path)
    try:
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        out = []
        if "cookies" in tables:  # Chromium
            cols = _columns(con, "cookies")
            browser = guess_chromium_browser(path)
            for r in con.execute("SELECT rowid AS _rowid, * FROM cookies"):
                r = dict(r)
                enc = r.get("encrypted_value") or b""
                plain = r.get("value") or ""
                encrypted = bool(enc) and not plain
                ss = r.get("samesite", -1)
                out.append(new_cookie(
                    browser=browser, source_file=path, name=r.get("name", ""),
                    value=None if encrypted else plain,
                    value_encrypted=encrypted,
                    value_length=len(enc) if encrypted else len(plain),
                    domain=r.get("host_key", ""), path=r.get("path", "/"),
                    created=from_webkit(r.get("creation_utc")),
                    expires=from_webkit(r.get("expires_utc")) if r.get("has_expires", 1) else None,
                    last_accessed=from_webkit(r.get("last_access_utc")),
                    secure=bool(r.get("is_secure", r.get("secure", 0))),
                    http_only=bool(r.get("is_httponly", r.get("httponly", 0))),
                    same_site=SAMESITE_CHROMIUM.get(ss, str(ss)) if "samesite" in cols else "",
                    session=not bool(r.get("is_persistent", r.get("persistent", 1))),
                    host_only=not str(r.get("host_key", "")).startswith("."),
                    partition_key=r.get("top_frame_site_key", "") or "",
                    _kind="chromium", _rowid=r["_rowid"],
                ))
                if encrypted and enc[:3] in (b"v10", b"v11", b"v20"):
                    out[-1]["encryption"] = enc[:3].decode()
        elif "moz_cookies" in tables:  # Firefox
            for r in con.execute("SELECT rowid AS _rowid, * FROM moz_cookies"):
                r = dict(r)
                expiry = r.get("expiry")
                # Firefox 120+ stores expiry in ms; older versions in seconds
                if expiry and expiry > 10 ** 11:
                    expiry = expiry / 1000
                host = r.get("host", "")
                out.append(new_cookie(
                    browser="Mozilla Firefox", source_file=path, name=r.get("name", ""),
                    value=r.get("value", ""), domain=host, path=r.get("path", "/"),
                    created=from_unix((r.get("creationTime") or 0) / 1e6),
                    expires=from_unix(expiry),
                    last_accessed=from_unix((r.get("lastAccessed") or 0) / 1e6),
                    secure=bool(r.get("isSecure")), http_only=bool(r.get("isHttpOnly")),
                    same_site=SAMESITE_FIREFOX.get(r.get("sameSite"), str(r.get("sameSite", ""))),
                    session=bool(r.get("isSession", 0)),
                    host_only=not host.startswith("."),
                    partition_key=_ff_partition(r.get("originAttributes", "")),
                    _kind="firefox", _rowid=r["_rowid"],
                ))
        return out
    finally:
        con.close()
        shutil.rmtree(tmp, ignore_errors=True)


def _ff_partition(attrs):
    m = re.search(r"partitionKey=([^&]+)", attrs or "")
    return m.group(1) if m else ""


def parse_netscape(path, text):
    out = []
    for idx, line in enumerate(text.splitlines()):
        http_only = False
        if line.startswith("#HttpOnly_"):
            http_only = True
            line = line[len("#HttpOnly_"):]
        elif not line.strip() or line.startswith("#"):
            continue
        parts = line.rstrip("\r\n").split("\t")
        if len(parts) < 7:
            continue
        domain, include_sub, cpath, secure, expiry, name, value = parts[:7]
        try:
            exp = int(float(expiry))
        except ValueError:
            exp = 0
        out.append(new_cookie(
            browser="Netscape cookies.txt", source_file=path, name=name, value=value,
            domain=domain, path=cpath, expires=from_unix(exp), session=exp == 0,
            secure=secure.upper() == "TRUE", http_only=http_only,
            host_only=include_sub.upper() != "TRUE", _kind="netscape", _index=idx))
    return out


def parse_json(path, text):
    data = json.loads(text)
    if isinstance(data, dict):
        data = data.get("cookies", [])
    if not isinstance(data, list):
        return []
    out = []
    for idx, c in enumerate(data):
        if not isinstance(c, dict) or "name" not in c:
            continue
        exp = c.get("expirationDate", c.get("expires"))
        session = bool(c.get("session")) or exp in (None, -1, 0)
        ss = c.get("sameSite", "")
        ss = {"no_restriction": "None", "unspecified": "Unspecified", "lax": "Lax",
              "strict": "Strict"}.get(str(ss).lower(), str(ss)) if ss else ""
        out.append(new_cookie(
            browser="JSON export", source_file=path, name=c.get("name", ""),
            value=str(c.get("value", "")), domain=c.get("domain", ""), path=c.get("path", "/"),
            expires=None if session else from_unix(exp), session=session,
            secure=c.get("secure"), http_only=c.get("httpOnly"), same_site=ss,
            host_only=c.get("hostOnly", not str(c.get("domain", "")).startswith(".")),
            _kind="json", _index=idx))
    return out


def parse_inetcookies(path, text):
    """Legacy IE / Windows INetCookies text format: 9-line records ending in '*'."""
    lines = text.splitlines()
    out = []
    i = 0
    while i + 8 < len(lines):
        rec = lines[i:i + 9]
        if rec[8].strip() != "*":
            break
        name, value, hostpath, flags = rec[0], rec[1], rec[2], int(rec[3] or 0)
        exp = (int(rec[5]) << 32) | int(rec[4])
        cre = (int(rec[7]) << 32) | int(rec[6])
        host, _, p = hostpath.partition("/")
        out.append(new_cookie(
            browser="Internet Explorer / WinINet", source_file=path, name=name, value=value,
            domain=host, path="/" + p, expires=from_filetime(exp), created=from_filetime(cre),
            secure=bool(flags & 0x1), http_only=bool(flags & 0x2000),
            _kind="ie", _index=i))
        i += 9
    return out


def parse_binarycookies(path, data):
    if data[:4] != b"cook":
        return []
    n_pages = struct.unpack(">I", data[4:8])[0]
    sizes = struct.unpack(f">{n_pages}I", data[8:8 + 4 * n_pages])
    pos = 8 + 4 * n_pages
    out = []
    for size in sizes:
        page = data[pos:pos + size]
        pos += size
        n = struct.unpack("<I", page[4:8])[0]
        offsets = struct.unpack(f"<{n}I", page[8:8 + 4 * n])
        for off in offsets:
            c = page[off:]
            csize, _, flags, _, url_o, name_o, path_o, val_o = struct.unpack("<8I", c[:32])
            expiry, created = struct.unpack("<dd", c[40:56])

            def s(o):
                return c[o:c.index(b"\x00", o)].decode("utf-8", "replace")
            out.append(new_cookie(
                browser="Safari", source_file=path, name=s(name_o), value=s(val_o),
                domain=s(url_o), path=s(path_o), expires=from_mac(expiry),
                created=from_mac(created), secure=bool(flags & 1), http_only=bool(flags & 4),
                _kind="safari"))
    return out


def sniff_and_parse(path, show_values):
    try:
        with open(path, "rb") as f:
            head = f.read(16)
    except OSError as e:
        return None, f"cannot read ({e})"
    try:
        if head.startswith(b"SQLite format 3"):
            res = parse_sqlite(path, show_values)
            return (res, None) if res else (None, "SQLite file without a cookie table")
        if head.startswith(b"cook"):
            with open(path, "rb") as f:
                return parse_binarycookies(path, f.read()), None
        if os.path.getsize(path) > 50 * 1024 * 1024:
            return None, "skipped (too large for a text cookie file)"
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            text = f.read()
        stripped = text.lstrip()
        if stripped.startswith(("[", "{")):
            try:
                res = parse_json(path, text)
                return (res, None) if res else (None, "JSON without cookie entries")
            except json.JSONDecodeError:
                pass
        if "# Netscape HTTP Cookie File" in text[:200] or "# HTTP Cookie File" in text[:200] \
                or re.search(r"^\S+\t(TRUE|FALSE)\t\S*\t(TRUE|FALSE)\t\d+\t", text, re.M):
            return parse_netscape(path, text), None
        lines = text.splitlines()
        if len(lines) >= 9 and lines[8].strip() == "*":
            res = parse_inetcookies(path, text)
            if res:
                return res, None
        return None, "not a recognised cookie format"
    except Exception as e:  # keep scanning other files
        return None, f"parse error: {type(e).__name__}: {e}"


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------
def lookup(name):
    for pat, vendor, cat, desc in KNOWN_COOKIES:
        if fnmatch.fnmatchcase(name, pat):
            return vendor, cat, desc, "known"
    low = name.lower()
    for rx, cat, desc in HEURISTICS:
        if re.search(rx, low):
            return "", cat, desc, "heuristic"
    return "", "Unknown", "No match in the built-in database; purpose unknown.", "none"


def tracker_for_domain(domain):
    d = domain.lstrip(".").lower()
    parts = d.split(".")
    for i in range(len(parts) - 1):
        cand = ".".join(parts[i:])
        if cand in TRACKER_DOMAINS:
            return TRACKER_DOMAINS[cand]
    return ""


def value_shape(value, length, encrypted):
    if encrypted:
        return "encrypted by browser"
    if value is None or length == 0:
        return "empty"
    v = value
    if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", v):
        return "UUID"
    if re.fullmatch(r"eyJ[\w-]+\.[\w-]+\.[\w-]*", v):
        return "JWT (signed token)"
    if re.fullmatch(r"GA\d\.\d\.\d+\.\d+", v) or re.fullmatch(r"GS\d\.\d\..*", v):
        return "Google Analytics ID"
    if re.fullmatch(r"\d{9,13}", v):
        t = int(v) / (1000 if len(v) == 13 else 1)
        if 946684800 < t < 4102444800:
            return "Unix timestamp"
        return "numeric ID"
    if re.fullmatch(r"\d+", v):
        return "number"
    if v.lower() in ("true", "false", "yes", "no", "0", "1", "on", "off"):
        return "boolean flag"
    if v.startswith(("{", "[", "%7B", "%5B")):
        return "JSON / structured data"
    if re.fullmatch(r"[0-9a-fA-F]{16,}", v):
        return f"hex string ({len(v) * 4}-bit)"
    if re.fullmatch(r"[A-Za-z0-9+/=_-]{20,}", v):
        return "random/base64 token"
    if "%" in v or "=" in v or "&" in v or "|" in v:
        return "encoded key/value data"
    return "text"


def mask(value, encrypted):
    if encrypted:
        return "[encrypted]"
    if value is None:
        return ""
    if len(value) <= 8:
        return value
    return f"{value[:4]}…{value[-4:]}"


def analyse(c, show_values, site=None):
    vendor, cat, desc, basis = lookup(c["name"])
    tracker = tracker_for_domain(c["domain"])
    if tracker and not vendor:
        vendor = tracker
        if cat == "Unknown":
            cat, desc, basis = "Advertising", f"Set by tracking domain ({tracker}).", "domain"
    c.update(vendor=vendor, category=cat, description=desc, match_basis=basis,
             tracker_domain=tracker)

    if c["session"] or c["expires"] is None:
        c["lifetime"] = "session (deleted when browser closes)"
        remaining = None
    else:
        start = c["created"] or NOW
        c["lifetime"] = human_duration(c["expires"] - start)
        remaining = c["expires"] - NOW
    c["expired"] = remaining is not None and remaining.total_seconds() < 0
    c["expires_in"] = "expired" if c["expired"] else human_duration(remaining) if remaining else ""

    c["value_shape"] = value_shape(c["value"], c["value_length"], c["value_encrypted"])
    c["value_preview"] = mask(c["value"], c["value_encrypted"]) if show_values else ""
    if site:
        d = c["domain"].lstrip(".").lower()
        c["party"] = "first-party" if d == site or d.endswith("." + site) else "third-party"
    else:
        c["party"] = "likely third-party (tracker)" if tracker else ""

    f = []
    sensitive = cat in ("Authentication", "Strictly necessary", "Security")
    if c["expired"]:
        f.append("Expired but still stored")
    if c["secure"] is False:
        f.append("Not Secure — can be sent over plain HTTP")
    if c["http_only"] is False and sensitive and c["category"] != "Consent":
        f.append("Not HttpOnly — readable by page JavaScript (XSS risk)")
    if c["same_site"].startswith("None") and c["secure"] is False:
        f.append("SameSite=None without Secure — modern browsers reject this")
    if c["same_site"].startswith("None") and sensitive:
        f.append("SameSite=None on a session/auth cookie — sent on cross-site requests")
    if remaining and remaining.days > 400:
        f.append(f"Very long-lived ({human_duration(remaining)} left; browsers now cap at ~400 days)")
    if cat == "Advertising":
        f.append("Advertising / cross-site tracking")
    if c["value_length"] > 4096:
        f.append("Value larger than 4 KB")
    if c["name"].startswith("__Host-") and (c["secure"] is False or c["path"] != "/" or not c.get("host_only", True)):
        f.append("Violates __Host- prefix rules")
    c["findings"] = f
    return c


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------
CATEGORY_ALIASES = {
    "strictly necessary": "Strictly necessary", "necessary": "Strictly necessary",
    "essential": "Strictly necessary", "strict": "Strictly necessary",
    "authentication": "Authentication", "auth": "Authentication", "login": "Authentication",
    "security": "Security", "functional": "Functional", "preferences": "Functional",
    "preference": "Functional", "consent": "Consent", "analytics": "Analytics",
    "stats": "Analytics", "statistics": "Analytics", "advertising": "Advertising",
    "ads": "Advertising", "marketing": "Advertising", "tracking": "Advertising",
    "unknown": "Unknown",
}
DELETABLE_KINDS = {"chromium", "firefox", "netscape", "json", "ie"}
BROWSER_PROCESSES = {
    "Google Chrome": ["chrome", "google chrome"], "Microsoft Edge": ["msedge", "microsoft edge"],
    "Brave": ["brave", "brave browser"], "Opera": ["opera"], "Vivaldi": ["vivaldi"],
    "Chromium": ["chromium", "chromium-browser"], "Mozilla Firefox": ["firefox"],
}
BROWSER_PROCESSES["Chromium-based browser"] = sorted(
    {p for k, v in BROWSER_PROCESSES.items() if k != "Mozilla Firefox" for p in v})


def parse_categories(text):
    cats = set()
    for raw in (text or "").split(","):
        key = raw.strip().lower()
        if not key:
            continue
        if key == "all":
            cats.update(CATEGORY_ORDER)
        elif key in CATEGORY_ALIASES:
            cats.add(CATEGORY_ALIASES[key])
        else:
            sys.exit(f"Unknown category '{raw.strip()}'. Choose from: "
                     + ", ".join(CATEGORY_ORDER) + ", all")
    return cats


def _domain_match(domain, wanted):
    d, w = domain.lstrip(".").lower(), wanted.lstrip(".").lower()
    return d == w or d.endswith("." + w)


def cookie_id(c):
    """Short ID that stays the same between runs as long as the cookie exists."""
    key = "|".join(str(x) for x in (os.path.abspath(c["source_file"]), c["domain"], c["name"],
                                    c["path"], c["partition_key"], c["_rowid"], c["_index"]))
    return hashlib.sha1(key.encode("utf-8", "replace")).hexdigest()[:8]


def select_for_deletion(cookies, categories=(), expired=False, names=(), domains=(), ids=(),
                        keep_domains=()):
    """Every selector that is given must match (AND); values within one selector are ORed."""
    names = [n.lower() for n in names]
    ids = {i.lower() for i in ids}
    if not (categories or expired or names or domains or ids):
        return []
    chosen = []
    for c in cookies:
        if categories and c["category"] not in categories:
            continue
        if expired and not c["expired"]:
            continue
        if names and not any(fnmatch.fnmatchcase(c["name"].lower(), n) for n in names):
            continue
        if domains and not any(_domain_match(c["domain"], d) for d in domains):
            continue
        if ids and c["id"] not in ids:
            continue
        if any(_domain_match(c["domain"], k) for k in keep_domains):
            continue
        chosen.append(c)
    return chosen


def report_unmatched(cookies, names, domains, ids):
    for i in ids:
        if not any(c["id"] == i.lower() for c in cookies):
            print(f"  ! No cookie has ID '{i}' (IDs change if the cookie was deleted or re-set).")
    for n in names:
        if not any(fnmatch.fnmatchcase(c["name"].lower(), n.lower()) for c in cookies):
            print(f"  ! No cookie is named '{n}'.")
    for d in domains:
        if not any(_domain_match(c["domain"], d) for c in cookies):
            print(f"  ! No cookies found for domain '{d}'.")


def running_processes():
    import subprocess
    try:
        if platform.system() == "Windows":
            out = subprocess.run(["tasklist", "/fo", "csv", "/nh"], capture_output=True,
                                 text=True, timeout=10).stdout
            names = [l.split('","')[0].strip('"') for l in out.splitlines() if l]
        else:
            out = subprocess.run(["ps", "-A", "-o", "comm="], capture_output=True,
                                 text=True, timeout=10).stdout
            names = [os.path.basename(l.strip()) for l in out.splitlines()]
    except Exception:
        return set()
    return {re.sub(r"\.exe$", "", n.lower()) for n in names if n}


def browser_running(browser, procs):
    for p in BROWSER_PROCESSES.get(browser, []):
        for n in procs:
            if n == p or n.startswith(p + " ") or n.startswith(p + "_"):
                return True
    return False


def backup_file(path, stamp):
    made = []
    for ext in ("", "-wal", "-shm", "-journal"):
        src = path + ext
        if os.path.exists(src):
            dst = f"{src}.bak-{stamp}"
            shutil.copy2(src, dst)
            made.append(dst)
    return made[0] if made else None


def _sqlite_delete(path, table, rowids):
    try:
        con = sqlite3.connect(path, timeout=2)
        con.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as e:
        raise RuntimeError(f"file is locked ({e}) — fully close the browser and retry")
    try:
        cur = con.executemany(f"DELETE FROM {table} WHERE rowid = ?", [(r,) for r in rowids])
        con.commit()
        try:
            con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error:
            pass
        return cur.rowcount
    finally:
        con.close()


def _text_rewrite(path, kind, indexes):
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        text = f.read()
    drop = set(indexes)
    if kind == "netscape":
        lines = text.splitlines(keepends=True)
        new = "".join(l for i, l in enumerate(lines) if i not in drop)
    elif kind == "json":
        data = json.loads(text)
        lst = data.get("cookies", []) if isinstance(data, dict) else data
        kept = [c for i, c in enumerate(lst) if i not in drop]
        if isinstance(data, dict):
            data["cookies"] = kept
        else:
            data = kept
        new = json.dumps(data, indent=2, ensure_ascii=False)
    elif kind == "ie":
        lines = text.splitlines(keepends=True)
        new = "".join(l for n, l in enumerate(lines) if (n - n % 9) not in drop)
        if not new.strip():
            os.remove(path)
            return len(drop)
    else:
        raise RuntimeError("unsupported format")
    tmp = path + ".tmp-cookieinspector"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(new)
    os.replace(tmp, path)
    return len(drop)


def delete_cookies(targets, backup=True, force=False):
    """Delete the given cookie records from their source files. Returns per-file results."""
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    procs = running_processes()
    by_file = defaultdict(list)
    for c in targets:
        by_file[c["source_file"]].append(c)
    results = []
    for path, cs in sorted(by_file.items()):
        kind, browser = cs[0]["_kind"], cs[0]["browser"]
        res = {"file": path, "browser": browser, "requested": len(cs), "deleted": 0,
               "backup": None, "error": None}
        try:
            if kind not in DELETABLE_KINDS:
                raise RuntimeError(f"skipped — {browser} cookie files can't be edited by this tool; "
                                   "delete these in the browser's own settings")
            if not force and browser_running(browser, procs):
                raise RuntimeError(f"{browser} appears to be running — close it completely "
                                   "(or pass --force) and retry")
            if kind in ("chromium", "firefox"):  # check the lock before backing up
                _sqlite_delete(path, "cookies" if kind == "chromium" else "moz_cookies", [])
            if backup:
                res["backup"] = backup_file(path, stamp)
            if kind == "chromium":
                n = _sqlite_delete(path, "cookies", [c["_rowid"] for c in cs])
            elif kind == "firefox":
                n = _sqlite_delete(path, "moz_cookies", [c["_rowid"] for c in cs])
            else:
                n = _text_rewrite(path, kind, [c["_index"] for c in cs])
            res["deleted"] = n
            for c in cs:
                c["deletion"] = "deleted"
        except Exception as e:
            res["error"] = str(e)
            for c in cs:
                c["deletion"] = f"failed: {e}"
        results.append(res)
    return results


def print_deletion_plan(chosen, applying):
    verb = "Deleting" if applying else "Would delete (dry run)"
    print(f"\n{verb}: {len(chosen)} cookie(s) from "
          f"{len({c['source_file'] for c in chosen})} file(s)")
    by_cat = defaultdict(list)
    for c in chosen:
        by_cat[c["category"]].append(c)
    if len(chosen) <= 25:
        for c in chosen:
            print(f"  [{c['id']}] {c['category']:<18} {c['name']:<28} "
                  f"{c['domain'].lstrip('.')}{c['path']}  ({c['browser']})")
        by_cat = {}
    for cat in CATEGORY_ORDER:
        if cat in by_cat:
            items = by_cat[cat]
            sample = ", ".join(f"{c['name']} ({c['domain'].lstrip('.')})" for c in items[:6])
            more = f", … +{len(items) - 6} more" if len(items) > 6 else ""
            print(f"  {cat:<20} {len(items):>4}  {sample}{more}")
    if any(c["category"] in ("Authentication", "Strictly necessary") for c in chosen):
        print("  ! This includes sign-in / session cookies: you will be logged out of those sites.")
    bad = [c for c in chosen if c["_kind"] not in DELETABLE_KINDS]
    if bad:
        print(f"  ! {len(bad)} cookie(s) are in read-only formats (e.g. Safari) and will be skipped.")


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
FIELDS = ["id", "browser", "domain", "name", "vendor", "category", "description", "match_basis",
          "party", "path", "created", "last_accessed", "expires", "expires_in", "lifetime",
          "session", "secure", "http_only", "same_site", "host_only", "partition_key",
          "value_shape", "value_length", "value_encrypted", "value_preview", "findings",
          "deletion", "source_file"]


def row_for_export(c):
    r = {}
    for k in FIELDS:
        v = c.get(k)
        if isinstance(v, dt.datetime):
            v = fmt_time(v)
        elif isinstance(v, list):
            v = "; ".join(v)
        r[k] = "" if v is None else v
    return r


def write_csv(cookies, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for c in cookies:
            w.writerow(row_for_export(c))


def write_json(cookies, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump([row_for_export(c) for c in cookies], f, indent=2, ensure_ascii=False)


CAT_COLOURS = {"Strictly necessary": "#2e7d32", "Authentication": "#6a1b9a", "Security": "#00838f",
               "Functional": "#1565c0", "Consent": "#5d4037", "Analytics": "#ef6c00",
               "Advertising": "#c62828", "Unknown": "#616161"}


def write_html(cookies, path, scanned, skipped, root):
    e = html.escape
    del_label = {"selected": ("would delete", "sel"), "deleted": ("deleted", "done")}
    script = os.path.basename(sys.argv[0]) or "cookie_inspector.py"
    base_raw = f'python {script} "{root}"'

    cats = Counter(c["category"] for c in cookies)
    domains = Counter(c["domain"].lstrip(".") for c in cookies)
    browsers = Counter(c["browser"] for c in cookies)
    findings = Counter(f.split(" (")[0].split(" —")[0] for c in cookies for f in c["findings"])
    vendors = Counter(c["vendor"] for c in cookies if c["vendor"] and not c["vendor"].startswith("("))
    n_track = sum(1 for c in cookies if c["category"] == "Advertising")
    n_sess = sum(1 for c in cookies if c["session"])
    n_exp = sum(1 for c in cookies if c["expired"])

    def bar_rows(counter, colour=None, limit=15):
        if not counter:
            return "<p class='muted'>None</p>"
        mx = max(counter.values())
        rows = []
        for k, v in counter.most_common(limit):
            col = colour or CAT_COLOURS.get(k, "#546e7a")
            rows.append(f"<div class='bar'><span class='lbl'>{e(str(k))}</span>"
                        f"<span class='track'><span style='width:{100 * v / mx:.0f}%;background:{col}'></span></span>"
                        f"<span class='num'>{v}</span></div>")
        return "".join(rows)

    cat_counter = Counter({k: cats[k] for k in CATEGORY_ORDER if cats.get(k)})

    rows = []
    for i, c in enumerate(cookies):
        dom = c["domain"].lstrip(".")
        flags = []
        flags.append("<b class='ok'>Secure</b>" if c["secure"] else "<b class='bad'>not Secure</b>" if c["secure"] is False else "")
        flags.append("<b class='ok'>HttpOnly</b>" if c["http_only"] else "<b class='warn'>JS-readable</b>" if c["http_only"] is False else "")
        if c["same_site"]:
            flags.append(f"<b>SameSite {e(c['same_site'].split(' ')[0])}</b>")
        col = CAT_COLOURS.get(c["category"], "#616161")
        find = "".join(f"<li>{e(x)}</li>" for x in c["findings"])
        basis = {"known": "", "heuristic": " <i class='muted'>(guessed from name)</i>",
                 "domain": " <i class='muted'>(from domain)</i>", "none": ""}[c["match_basis"]]
        detail = (
            f"<div class='grid'>"
            f"<div><span>Path</span>{e(c['path'])}</div>"
            f"<div><span>Created</span>{e(fmt_time(c['created'])) or '—'}</div>"
            f"<div><span>Last accessed</span>{e(fmt_time(c['last_accessed'])) or '—'}</div>"
            f"<div><span>Expires</span>{e(fmt_time(c['expires'])) or 'end of session'}</div>"
            f"<div><span>Lifetime</span>{e(c['lifetime'])}</div>"
            f"<div><span>Value</span>{e(c['value_shape'])}, {c['value_length']} bytes"
            f"{(' — ' + e(c['value_preview'])) if c['value_preview'] else ''}</div>"
            f"<div><span>Scope</span>{'this host only' if c.get('host_only') else 'domain + subdomains'}"
            f"{(' · partitioned under ' + e(c['partition_key'])) if c['partition_key'] else ''}</div>"
            f"<div><span>Source</span>{e(c['browser'])} — <code>{e(os.path.relpath(c['source_file'], root))}</code></div>"
            f"<div><span>ID</span><code>{e(c['id'])}</code></div>"
            f"</div>{('<ul class=find>' + find + '</ul>') if find else ''}"
            f"<div class='acts'><button type='button' class='cp' data-cmd='{e(base_raw + ' --delete-id ' + c['id'] + ' --apply')}'>"
            f"Delete this cookie…</button>"
            f"<button type='button' class='cp' data-cmd='{e(base_raw + ' --delete-domain ' + chr(34) + dom + chr(34) + ' --apply')}'>"
            f"Delete all {e(dom)} cookies…</button>"
            f"<button type='button' class='cp' data-cmd='{e(base_raw + ' --delete ' + chr(34) + c['category'] + chr(34) + ' --delete-domain ' + chr(34) + dom + chr(34) + ' --apply')}'>"
            f"Delete {e(c['category'])} cookies for {e(dom)}…</button>"
            f"<pre class='cmdout' hidden></pre></div>")
        st = c.get("deletion", "")
        lbl, cls = del_label.get(st, ("delete failed", "fail") if st else ("", ""))
        badge = f" <em class='del {cls}' title='{e(st)}'>{lbl}</em>" if st else ""
        search = " ".join([c["id"], c["name"], c["domain"], c["vendor"], c["category"], c["description"],
                           c["browser"], " ".join(c["findings"])]).lower()
        rows.append(
            f"<details class='ck' data-cat='{e(c['category'])}' data-s='{e(search)}'"
            f" data-f='{1 if c['findings'] else 0}' data-d='{e(st)}' data-x='{1 if c['expired'] else 0}'>"
            f"<summary><span class='pill' style='background:{col}'>{e(c['category'])}</span>"
            f"<span class='nm'>{e(c['name'])}{badge}</span><span class='dm'>{e(c['domain'])}</span>"
            f"<span class='vd'>{e(c['vendor'])}</span>"
            f"<span class='ex'>{e(c['expires_in'] or 'session')}</span>"
            f"{'<span class=dot title=findings>●</span>' if c['findings'] else ''}</summary>"
            f"<p class='desc'>{e(c['description'])}{basis}</p>"
            f"<p class='flags'>{' '.join(x for x in flags if x)}</p>{detail}</details>")

    skipped_html = "".join(f"<li><code>{e(os.path.relpath(p, root))}</code> — {e(r)}</li>" for p, r in skipped[:200])
    cat_opts = "".join(f"<option>{e(k)}</option>" for k in CATEGORY_ORDER if cats.get(k))
    del_boxes = "".join(
        f"<label><input type='checkbox' class='delcat' value='{e(k)}'> {e(k)} <span class='muted'>({cats[k]})</span></label>"
        for k in CATEGORY_ORDER if cats.get(k))
    base_cmd = json.dumps(base_raw)
    any_del = any(c.get("deletion") for c in cookies)

    doc = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Cookie Report</title>
<style>
:root{{--bg:#f7f7f5;--card:#fff;--fg:#1d1d1f;--muted:#6b6b6b;--line:#e3e3e0;--accent:#1565c0}}
@media (prefers-color-scheme:dark){{:root{{--bg:#151517;--card:#1f1f22;--fg:#ececec;--muted:#9a9a9a;--line:#333;--accent:#64b5f6}}}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}}
main{{max-width:1100px;margin:0 auto;padding:24px 16px 60px}}h1{{margin:0 0 4px;font-size:26px}}h2{{font-size:16px;margin:0 0 12px}}
.muted{{color:var(--muted)}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:20px 0}}
.card{{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px}}.card b{{display:block;font-size:24px}}
.cols{{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:12px;margin-bottom:20px}}
.bar{{display:grid;grid-template-columns:minmax(90px,40%) 1fr 40px;gap:8px;align-items:center;margin:4px 0}}
.lbl{{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}.track{{background:var(--line);border-radius:4px;height:10px;overflow:hidden}}
.track span{{display:block;height:100%}}.num{{text-align:right;font-variant-numeric:tabular-nums}}
.tools{{display:flex;gap:8px;flex-wrap:wrap;margin:16px 0 10px;position:sticky;top:0;background:var(--bg);padding:8px 0;z-index:2}}
input,select{{font:inherit;padding:8px 10px;border:1px solid var(--line);border-radius:8px;background:var(--card);color:var(--fg)}}.tools input{{flex:1;min-width:180px}}.delbox input{{margin:0 6px 0 0;vertical-align:-1px}}
.ck{{background:var(--card);border:1px solid var(--line);border-radius:8px;margin:6px 0}}
.ck summary{{display:grid;grid-template-columns:130px minmax(100px,1.2fr) minmax(100px,1.2fr) minmax(80px,1fr) 100px 14px;gap:10px;align-items:center;padding:10px 12px;cursor:pointer;list-style:none}}
.ck summary::-webkit-details-marker{{display:none}}.nm{{font-weight:600;word-break:break-all}}.dm,.vd,.ex{{color:var(--muted);word-break:break-all}}
.pill{{color:#fff;border-radius:999px;padding:2px 8px;font-size:12px;text-align:center;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.dot{{color:#e65100}}.desc{{margin:0 12px 6px}}.flags{{margin:0 12px 8px}}.flags b{{font-weight:500;font-size:12px;border:1px solid var(--line);border-radius:6px;padding:1px 6px;margin-right:4px}}
.ok{{color:#2e7d32}}.bad{{color:#c62828}}.warn{{color:#e65100}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:6px 16px;margin:0 12px 10px;font-size:13px}}
.grid span{{display:block;color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.04em}}
.find{{margin:0 12px 12px;padding:8px 8px 8px 26px;background:rgba(230,81,0,.08);border-radius:6px;color:#e65100}}
code{{font-size:12px;word-break:break-all}}
.del{{font-style:normal;font-weight:500;font-size:11px;border-radius:4px;padding:1px 5px;margin-left:6px;white-space:nowrap}}
.del.sel{{background:rgba(198,40,40,.12);color:#c62828}}.del.done{{background:#c62828;color:#fff}}.del.fail{{background:#616161;color:#fff}}
.delbox{{display:grid;gap:4px}}
.acts{{display:flex;flex-wrap:wrap;gap:6px;margin:0 12px 12px}}.acts button{{font-size:12px;padding:4px 10px}}
.cmdout{{flex-basis:100%;white-space:pre-wrap;word-break:break-all;background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:8px;font-size:12px;margin:2px 0 0}}.delbox label{{cursor:pointer}}
pre#delcmd{{white-space:pre-wrap;word-break:break-all;background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:8px;font-size:12px;margin:0 0 8px}}
button{{font:inherit;padding:6px 12px;border-radius:8px;border:1px solid var(--line);background:var(--card);color:var(--fg);cursor:pointer}}
@media (max-width:700px){{.ck summary{{grid-template-columns:1fr 1fr}}.vd,.ex,.dot{{display:none}}}}
</style></head><body><main>
<h1>Cookie Report</h1>
<p class="muted">Scanned <code>{e(root)}</code> on {e(NOW.astimezone().strftime('%Y-%m-%d %H:%M %Z'))} · {scanned} files examined · {len(skipped)} skipped</p>
<div class="cards">
<div class="card"><b>{len(cookies)}</b>cookies</div><div class="card"><b>{len(domains)}</b>domains</div>
<div class="card"><b>{n_track}</b>advertising / tracking</div><div class="card"><b>{n_sess}</b>session-only</div>
<div class="card"><b>{n_exp}</b>expired</div><div class="card"><b>{len(browsers)}</b>sources</div></div>
<div class="cols">
<div class="card"><h2>By purpose</h2>{bar_rows(cat_counter)}</div>
<div class="card"><h2>Top domains</h2>{bar_rows(domains, '#546e7a')}</div>
<div class="card"><h2>Top vendors</h2>{bar_rows(vendors, '#455a64')}</div>
<div class="card"><h2>Findings</h2>{bar_rows(findings, '#e65100')}</div>
<div class="card"><h2>Delete cookies</h2>
<p class="muted" style="margin:0 0 8px">Tick the categories to remove; copy the command and run it with the browser closed. A backup is made automatically. To delete a single cookie or one domain, open that cookie in the list below.</p>
<div class="delbox">{del_boxes}<label><input type='checkbox' id='delexp'> Only expired cookies <span class='muted'>({n_exp})</span></label></div>
<p id="delinfo" class="muted" style="margin:8px 0 4px"></p>
<pre id="delcmd"></pre><button id="delcopy" type="button">Copy command</button>
</div>
</div>
<div class="tools"><input id="q" placeholder="Filter by name, domain, vendor, finding…">
<select id="cat"><option value="">All purposes</option>{cat_opts}</select>
<select id="fl"><option value="">All cookies</option><option value="1">With findings only</option>{"<option value='d'>Marked for deletion</option>" if any_del else ""}</select>
<span id="cnt" class="muted" style="align-self:center"></span></div>
<div id="list">{''.join(rows)}</div>
{('<details class=card style="margin-top:20px"><summary>Skipped files</summary><ul>' + skipped_html + '</ul></details>') if skipped else ''}
<p class="muted" style="margin-top:24px">Purpose descriptions come from a built-in list of well-known cookies; entries marked "guessed" are inferred from the cookie name and should be checked. Cookie values are never decrypted.</p>
</main><script>
const q=document.getElementById('q'),cat=document.getElementById('cat'),fl=document.getElementById('fl'),cnt=document.getElementById('cnt');
const items=[...document.querySelectorAll('.ck')];
function run(){{const t=q.value.toLowerCase().trim(),c=cat.value,f=fl.value;let n=0;
for(const el of items){{const ok=(!t||el.dataset.s.includes(t))&&(!c||el.dataset.cat===c)&&(!f||(f==='d'?el.dataset.d!=='':el.dataset.f==='1'));el.style.display=ok?'':'none';if(ok)n++;}}
cnt.textContent=n+' shown';}}
[q,cat,fl].forEach(x=>x.addEventListener('input',run));run();
const BASE={base_cmd},boxes=[...document.querySelectorAll('.delcat')],dexp=document.getElementById('delexp'),dcmd=document.getElementById('delcmd'),dinfo=document.getElementById('delinfo');
function mk(){{const cs=boxes.filter(b=>b.checked).map(b=>b.value);
const n=items.filter(el=>(!cs.length||cs.includes(el.dataset.cat))&&(!dexp.checked||el.dataset.x==='1')).length;
if(!cs.length&&!dexp.checked){{dcmd.textContent='Tick at least one category';dinfo.textContent='';return;}}
let s=BASE;if(cs.length)s+=' --delete "'+cs.join(',')+'"';if(dexp.checked)s+=' --delete-expired';
dcmd.textContent=s+' --apply';
const risky=cs.includes('Authentication')||cs.includes('Strictly necessary');
dinfo.textContent=n+' cookie(s) selected'+(risky?' — includes sign-in cookies; you will be logged out of those sites':'');}}
[...boxes,dexp].forEach(b=>b.addEventListener('change',mk));mk();
function copyText(t){{try{{navigator.clipboard.writeText(t).catch(()=>fallback(t));}}catch(_){{fallback(t);}}}}
function fallback(t){{const a=document.createElement('textarea');a.value=t;document.body.appendChild(a);a.select();try{{document.execCommand('copy');}}catch(_){{}}a.remove();}}
document.getElementById('delcopy').addEventListener('click',()=>copyText(dcmd.textContent));
document.addEventListener('click',ev=>{{const b=ev.target.closest('.cp');if(!b)return;
const out=b.parentElement.querySelector('.cmdout');out.hidden=false;out.textContent=b.dataset.cmd+'\\n(copied — run it with the browser closed; leave off --apply to preview)';copyText(b.dataset.cmd);}});
</script></body></html>"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(doc)


def print_summary(cookies, skipped):
    print(f"\nFound {len(cookies)} cookies across {len({c['domain'] for c in cookies})} domains.")
    cats = Counter(c["category"] for c in cookies)
    for k in CATEGORY_ORDER:
        if cats.get(k):
            print(f"  {k:<20} {cats[k]}")
    flagged = sum(1 for c in cookies if c["findings"])
    print(f"  {'with findings':<20} {flagged}")
    if skipped:
        print(f"Skipped {len(skipped)} file(s) that were not cookie stores.")


def print_locations():
    home = os.path.expanduser("~")
    sysname = platform.system()
    if sysname == "Windows":
        la, ra = os.environ.get("LOCALAPPDATA", ""), os.environ.get("APPDATA", "")
        locs = [rf"{la}\Google\Chrome\User Data\<Profile>\Network\Cookies",
                rf"{la}\Microsoft\Edge\User Data\<Profile>\Network\Cookies",
                rf"{la}\BraveSoftware\Brave-Browser\User Data\<Profile>\Network\Cookies",
                rf"{ra}\Mozilla\Firefox\Profiles\<profile>\cookies.sqlite",
                rf"{la}\Microsoft\Windows\INetCookies"]
    elif sysname == "Darwin":
        locs = [f"{home}/Library/Application Support/Google/Chrome/<Profile>/Cookies",
                f"{home}/Library/Application Support/Microsoft Edge/<Profile>/Cookies",
                f"{home}/Library/Application Support/BraveSoftware/Brave-Browser/<Profile>/Cookies",
                f"{home}/Library/Application Support/Firefox/Profiles/<profile>/cookies.sqlite",
                f"{home}/Library/Containers/com.apple.Safari/Data/Library/Cookies/Cookies.binarycookies"]
    else:
        locs = [f"{home}/.config/google-chrome/<Profile>/Cookies",
                f"{home}/.config/microsoft-edge/<Profile>/Cookies",
                f"{home}/.config/BraveSoftware/Brave-Browser/<Profile>/Cookies",
                f"{home}/.mozilla/firefox/<profile>/cookies.sqlite"]
    print("Typical cookie store locations on this system (<Profile> is usually 'Default'):")
    for l in locs:
        print("  " + l)
    print("\nTip: copy the file(s) into a folder and point this tool at that folder.")


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Describe every cookie in a folder of cookie stores.")
    ap.add_argument("path", nargs="?", help="Folder (searched recursively) or a single cookie file")
    ap.add_argument("-o", "--output", default="cookie_report.html", help="HTML report path")
    ap.add_argument("--csv", help="Also write a CSV file")
    ap.add_argument("--json", help="Also write a JSON file")
    ap.add_argument("--show-values", action="store_true", help="Include masked value previews (first/last 4 chars)")
    ap.add_argument("--site", help="Your site's domain, to label cookies first- vs third-party")
    ap.add_argument("--locations", action="store_true", help="Show where browsers store cookies, then exit")
    g = ap.add_argument_group("deletion")
    g.add_argument("--delete", metavar="CATEGORIES",
                   help="Comma-separated categories to delete, e.g. Advertising,Analytics (or 'all')")
    g.add_argument("--delete-expired", action="store_true",
                   help="Only expired cookies (on its own: every expired cookie)")
    g.add_argument("--delete-name", action="append", default=[], metavar="NAME",
                   help="Cookie name to delete; wildcards like '_ga*' allowed (repeatable)")
    g.add_argument("--delete-domain", action="append", default=[], metavar="DOMAIN",
                   help="Delete cookies for this domain and its subdomains (repeatable)")
    g.add_argument("--delete-id", action="append", default=[], metavar="ID",
                   help="Delete the cookie with this ID, shown in the report (repeatable)")
    g.add_argument("--keep-domain", action="append", default=[], metavar="DOMAIN",
                   help="Never delete cookies for this domain or its subdomains (repeatable)")
    g.add_argument("--apply", action="store_true", help="Actually delete (without it: dry run)")
    g.add_argument("--yes", "-y", action="store_true", help="Don't ask for confirmation")
    g.add_argument("--no-backup", action="store_true", help="Don't back up files before changing them")
    g.add_argument("--force", action="store_true", help="Delete even if the browser seems to be running")
    args = ap.parse_args()

    if args.locations:
        print_locations()
        return
    if not args.path:
        ap.error("give a folder or file to scan (or --locations)")
    root = os.path.abspath(args.path)
    if not os.path.exists(root):
        sys.exit(f"Path not found: {root}")

    files = [root] if os.path.isfile(root) else [
        os.path.join(d, f) for d, _, fs in os.walk(root) for f in fs
        if not f.endswith(("-wal", "-shm", "-journal", ".tmp-cookieinspector"))
        and not re.search(r"\.bak-\d{8}-\d{6}$", f)]
    base = root if os.path.isdir(root) else os.path.dirname(root)

    cookies, skipped = [], []
    for p in sorted(files):
        res, err = sniff_and_parse(p, args.show_values)
        if res:
            print(f"  ✓ {os.path.relpath(p, base)}: {len(res)} cookies ({res[0]['browser']})")
            cookies.extend(res)
        else:
            skipped.append((p, err))

    site = args.site.lower().lstrip(".") if args.site else None
    cookies = [analyse(c, args.show_values, site) for c in cookies]
    for c in cookies:
        c["id"] = cookie_id(c)
    cookies.sort(key=lambda c: (CATEGORY_ORDER.index(c["category"]), c["domain"].lstrip("."), c["name"]))

    del_results = None
    if args.delete or args.delete_expired or args.delete_name or args.delete_domain or args.delete_id:
        cats = parse_categories(args.delete)
        chosen = select_for_deletion(cookies, cats, args.delete_expired, args.delete_name,
                                     args.delete_domain, args.delete_id, args.keep_domain)
        print_deletion_plan(chosen, args.apply)
        report_unmatched(cookies, args.delete_name, args.delete_domain, args.delete_id)
        for c in chosen:
            c["deletion"] = "selected"
        if chosen and args.apply:
            if not args.yes:
                if not sys.stdin.isatty():
                    sys.exit("Refusing to delete without confirmation; add --yes.")
                ans = input(f"Delete {len(chosen)} cookie(s)? Close your browsers first. [y/N] ")
                if ans.strip().lower() not in ("y", "yes"):
                    print("Cancelled; nothing was deleted.")
                    chosen = []
                    for c in cookies:
                        c["deletion"] = ""
            if chosen:
                del_results = delete_cookies(chosen, backup=not args.no_backup, force=args.force)
        elif chosen:
            print("Dry run only. Add --apply to delete these cookies.")

    write_html(cookies, args.output, len(files), skipped, base)
    if args.csv:
        write_csv(cookies, args.csv)
    if args.json:
        write_json(cookies, args.json)
    print_summary(cookies, skipped)
    if del_results:
        print("\nDeletion results:")
        for r in del_results:
            rel = os.path.relpath(r["file"], base)
            if r["error"]:
                print(f"  ✗ {rel}: {r['error']}")
            else:
                print(f"  ✓ {rel}: deleted {r['deleted']} of {r['requested']}"
                      + (f"  (backup: {os.path.basename(r['backup'])})" if r["backup"] else ""))
        if any(r["backup"] for r in del_results):
            print("  To undo, close the browser and copy a .bak-<time> file back over the original "
                  "(do the same for any -wal/-shm backups).")
    print(f"\nHTML report: {os.path.abspath(args.output)}")
    if args.csv:
        print(f"CSV:         {os.path.abspath(args.csv)}")
    if args.json:
        print(f"JSON:        {os.path.abspath(args.json)}")


if __name__ == "__main__":
    main()
