#!/usr/bin/env python3
"""Delete tracking, advertising and analytics cookies from Chrome's Default profile.

Keeps sign-in, security and preference cookies, plus a few cookies kept on purpose
(Apple dssid2, all LinkedIn cookies, YouTube VISITOR_INFO1_LIVE).

    python3 clean_trackers.py            # dry run: list what would be deleted
    python3 clean_trackers.py --apply    # delete
    python3 clean_trackers.py --apply --quit-chrome   # quit Chrome, delete, reopen Chrome

Without --quit-chrome, refuses to touch the file while Chrome is running (exit code 2).
Each --apply run appends a line to cleanup.log next to this script.
"""
import argparse
import datetime
import fnmatch
import os
import sqlite3
import subprocess
import sys
import time

COOKIES = os.path.expanduser(
    "~/Library/Application Support/Google/Chrome/Default/Cookies")
LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cleanup.log")

# Cookie names that are tracking / ads / analytics wherever they appear.
TRACKER_NAMES = [
    # Google Analytics / Ads / DoubleClick
    "_ga", "_ga_*", "_gid", "_gat*", "_gcl_*", "IDE", "DSID", "test_cookie",
    "__gads", "__gpi", "__eoi", "OTZ",
    # YouTube session stats (VISITOR_INFO1_LIVE is kept below)
    "YSC",
    # Meta, Reddit, Microsoft, TikTok, Pinterest, Snap
    "_fbp", "_fbc", "_rdt_uuid", "_uetsid", "_uetvid", "_clck", "_clsk",
    "_tt_enable_cookie", "_ttp", "_pin_unauth", "_scid",
    # Hotjar, HubSpot, Segment, Mixpanel, Optimizely, VWO, Adobe
    "_hj*", "__hstc", "__hssc", "__hssrc", "hubspotutk", "ajs_anonymous_id",
    "mp_*_mixpanel", "stape_mixpanel_*", "optimizelyEndUserId",
    "vwo_uuid", "_vwo_*", "AMCV_*", "AMCVS_*", "s_ecid",
    # GitHub analytics
    "_octo",
    # Piano / Cxense (paywall + reader tracking)
    "__pat", "__pvi", "__tac", "__tae", "__tbc", "xbc", "_pcid", "_pctx",
    "_pcus", "_pcfe", "_pc_*", "_pn_*", "_pprv", "cX_P",
    # Ad identity (LiveRamp, Prebid SharedID, Braze, Sailthru)
    "_lr_*", "_pubcid*", "ab.storage.*", "sailthru_visitor", "sailthru_content",
    # Analytics (Snowplow, Tealium, Chartbeat, Datadog RUM, Azure App Insights)
    "_sp_id.*", "_sp_ses.*", "_sp_su", "utag_main", "_cb", "_cb_svref",
    "_chartbeat2", "_dd_s*", "ai_user", "ai_session", "csparkW_ga*",
    # Site-specific visitor / A/B-test IDs
    "ab_uuid", "_scor_uid", "_ml_id", "nxt_last_visit", "auuid", "lastfpvisit*",
    "ca_id", "nyt-a", "nyt-jkidd", "jkidd-p", "anonDeviceId", "ubid-main",
    "csm-hit", "nfvdid", "anthropic-device-id", "sophi_demeter", "bc_tstgrp",
    "vip-go-seg", "sspw-template-test",
]

# Generic names that are trackers only on these sites.
SCOPED_NAMES = {
    ("TR", "sp"): ["wsj.com", "marketwatch.com", "barrons.com", "dowjones.com"],
}

# Every cookie from these ad / tracking hosts.
TRACKER_HOSTS = [
    "doubleclick.net", "adsrvr.org", "google-analytics.com", "amazon-adsystem.com",
    "adnxs.com", "criteo.com", "criteo.net", "demdex.net", "rlcdn.com",
    "html-load.cc", "taboola.com", "outbrain.com", "scorecardresearch.com",
    "rubiconproject.com", "pubmatic.com", "casalemedia.com", "openx.net",
    "bluekai.com", "tapad.com", "mathtag.com", "3lift.com", "quantserve.com",
    "agkn.com", "everesttech.net", "bidswitch.net", "smartadserver.com",
    "yieldmo.com", "sharethrough.com", "indexww.com", "clarity.ms", "hotjar.com",
]

# Kept on purpose even if a rule above matches.
KEEP = [
    ("apple.com", "dssid2"),
    ("linkedin.com", "*"),
    ("youtube.com", "VISITOR_INFO1_LIVE"),
]


def on_site(host, site):
    host = host.lstrip(".")
    return host == site or host.endswith("." + site)


def is_tracker(host, name):
    if any(on_site(host, s) and fnmatch.fnmatchcase(name, n) for s, n in KEEP):
        return False
    if any(on_site(host, h) for h in TRACKER_HOSTS):
        return True
    if any(fnmatch.fnmatchcase(name, p) for p in TRACKER_NAMES):
        return True
    for names, sites in SCOPED_NAMES.items():
        if name in names and any(on_site(host, s) for s in sites):
            return True
    return False


def chrome_running():
    return subprocess.run(["pgrep", "-x", "Google Chrome"],
                          capture_output=True).returncode == 0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="really delete")
    ap.add_argument("--quit-chrome", action="store_true",
                    help="with --apply: quit Chrome first if running, reopen it afterwards")
    args = ap.parse_args()

    if not os.path.exists(COOKIES):
        print(f"Chrome cookie file not found: {COOKIES}")
        return 1
    reopen = False
    if args.apply and chrome_running():
        if not args.quit_chrome:
            print("Chrome is running; nothing deleted. Quit Chrome (Cmd+Q) and run again.")
            return 2
        if not quit_chrome():
            print("Chrome did not quit within 30 seconds; nothing deleted.")
            return 2
        reopen = True
    try:
        return clean(args.apply)
    finally:
        if reopen:
            subprocess.run(["open", "-a", "Google Chrome"])
            print("Chrome reopened.")


def quit_chrome():
    subprocess.run(["osascript", "-e", 'tell application "Google Chrome" to quit'],
                   capture_output=True)
    for _ in range(30):
        time.sleep(1)
        if not chrome_running():
            time.sleep(2)  # let Chrome finish flushing its cookie file
            return True
    return False


def clean(apply):

    uri = f"file:{COOKIES}?mode=rw" if apply else f"file:{COOKIES}?mode=ro&immutable=1"
    con = sqlite3.connect(uri, uri=True)
    rows = con.execute("SELECT rowid, host_key, name FROM cookies").fetchall()
    hits = [(rid, h, n) for rid, h, n in rows if is_tracker(h, n)]

    for _, h, n in sorted(hits, key=lambda r: (r[1].lstrip("."), r[2])):
        print(f"  {h.lstrip('.')}  {n}")
    total = len(rows)

    if not apply:
        print(f"Dry run: {len(hits)} of {total} cookies would be deleted. Use --apply to delete.")
        return 0

    with con:
        con.executemany("DELETE FROM cookies WHERE rowid = ?", [(r[0],) for r in hits])
    after = con.execute("SELECT COUNT(*) FROM cookies").fetchone()[0]
    ok = con.execute("PRAGMA integrity_check").fetchone()[0]
    con.close()

    msg = f"Deleted {len(hits)} of {total} cookies; {after} left; integrity {ok}."
    print(msg)
    with open(LOG, "a") as f:
        f.write(f"{datetime.datetime.now():%Y-%m-%d %H:%M}  {msg}\n")
    return 0 if ok == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
