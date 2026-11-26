import Foundation

/// What a cookie is for. Ported from cookie-inspector/clean_trackers.py.
enum CookieCategory: String, CaseIterable, Identifiable {
    case advertising = "Advertising"
    case analytics = "Analytics"
    case signIn = "Sign-in"
    case other = "Other"

    var id: String { rawValue }

    /// Advertising and analytics cookies are the ones the app deletes.
    var isTracker: Bool { self == .advertising || self == .analytics }
}

enum TrackerRules {
    /// Every cookie from these hosts is advertising / tracking.
    static let trackerHosts: [String] = [
        "doubleclick.net", "adsrvr.org", "google-analytics.com", "googlesyndication.com",
        "googleadservices.com", "amazon-adsystem.com", "adnxs.com", "criteo.com", "criteo.net",
        "demdex.net", "rlcdn.com", "html-load.cc", "taboola.com", "outbrain.com",
        "scorecardresearch.com", "rubiconproject.com", "pubmatic.com", "casalemedia.com",
        "openx.net", "bluekai.com", "tapad.com", "mathtag.com", "3lift.com", "quantserve.com",
        "agkn.com", "everesttech.net", "bidswitch.net", "smartadserver.com", "yieldmo.com",
        "sharethrough.com", "indexww.com", "clarity.ms", "hotjar.com", "seedtag.com",
        "aditude.io",
    ]

    static let advertisingNames: [String] = [
        "IDE", "DSID", "test_cookie", "__gads", "__gpi", "__eoi", "_gcl_*",
        "_fbp", "_fbc", "_rdt_uuid", "_uetsid", "_uetvid", "_tt_enable_cookie", "_ttp",
        "_pin_unauth", "_scid", "_lr_*", "_pubcid*", "ab.storage.*", "sailthru_visitor",
        "sailthru_content", "tatari-session-cookie", "checkout_entry_point",
    ]

    static let analyticsNames: [String] = [
        "_ga", "_ga_*", "_gid", "_gat*", "OTZ", "YSC", "_clck", "_clsk", "_hj*",
        "__hstc", "__hssc", "__hssrc", "hubspotutk", "ajs_anonymous_id", "mp_*_mixpanel",
        "stape_mixpanel_*", "optimizelyEndUserId", "vwo_uuid", "_vwo_*", "AMCV_*", "AMCVS_*",
        "s_ecid", "_octo", "__pat", "__pvi", "__tac", "__tae", "__tbc", "xbc", "_pcid",
        "_pctx", "_pcus", "_pcfe", "_pc_*", "_pn_*", "_pprv", "cX_P", "_sp_id.*", "_sp_ses.*",
        "_sp_su", "utag_main", "_cb", "_cb_svref", "_chartbeat2", "_dd_s*", "ai_user",
        "ai_session", "csparkW_ga*", "ab_uuid", "_scor_uid", "_ml_id", "nxt_last_visit",
        "auuid", "lastfpvisit*", "ca_id", "nyt-a", "nyt-jkidd", "jkidd-p", "anonDeviceId",
        "csm-hit", "sophi_demeter", "bc_tstgrp", "vip-go-seg", "sspw-template-test",
        "sessionIndex",
    ]

    /// Generic names that are trackers only on these sites.
    static let scopedAnalytics: [(names: [String], sites: [String])] = [
        (["TR", "sp"], ["wsj.com", "marketwatch.com", "barrons.com", "dowjones.com"]),
    ]

    static let signInNames: [String] = [
        "SID", "HSID", "SSID", "APISID", "SAPISID", "LSID", "OSID", "LOGIN_INFO",
        "__Secure-*PSID*", "__Secure-*PAPISID", "__Host-*", "sessionKey*", "session-token",
        "at-main", "sst-main", "x-main", "NetflixId", "SecureNetflixId", "li_at", "user_session",
        "_gh_sess", "dotcom_user", "JSESSIONID", "*session*", "*Session*", "*sess", "*auth*",
        "*token*", "*Token*",
    ]

    /// Cookies the user chose to keep even though a tracker rule matches.
    static let keep: [(site: String, name: String)] = [
        ("apple.com", "dssid2"),
        ("linkedin.com", "*"),
        ("youtube.com", "VISITOR_INFO1_LIVE"),
    ]

    static func onSite(_ host: String, _ site: String) -> Bool {
        let h = host.hasPrefix(".") ? String(host.dropFirst()) : host
        return h == site || h.hasSuffix("." + site)
    }

    static func isKept(host: String, name: String) -> Bool {
        keep.contains { onSite(host, $0.site) && wildcard(name, $0.name) }
    }

    static func category(host: String, name: String) -> CookieCategory {
        if isKept(host: host, name: name) {
            return signInNames.contains { wildcard(name, $0) } ? .signIn : .other
        }
        if trackerHosts.contains(where: { onSite(host, $0) }) { return .advertising }
        if advertisingNames.contains(where: { wildcard(name, $0) }) { return .advertising }
        if analyticsNames.contains(where: { wildcard(name, $0) }) { return .analytics }
        for rule in scopedAnalytics where rule.names.contains(name) {
            if rule.sites.contains(where: { onSite(host, $0) }) { return .analytics }
        }
        if signInNames.contains(where: { wildcard(name, $0) }) { return .signIn }
        return .other
    }

    /// Registrable-ish site for grouping: "mail.google.com" -> "google.com".
    static func site(for host: String) -> String {
        let h = host.hasPrefix(".") ? String(host.dropFirst()) : host
        let parts = h.split(separator: ".").map(String.init)
        guard parts.count > 2 else { return h }
        let lastTwo = parts.suffix(2).joined(separator: ".")
        let twoLevel: Set<String> = ["co.uk", "com.au", "co.in", "co.jp", "com.br", "co.nz",
                                     "org.uk", "ac.uk", "com.sg", "co.za"]
        return twoLevel.contains(lastTwo) ? parts.suffix(3).joined(separator: ".") : lastTwo
    }

    /// Case-sensitive shell-style match supporting `*` and `?`.
    static func wildcard(_ text: String, _ pattern: String) -> Bool {
        let t = Array(text), p = Array(pattern)
        var ti = 0, pi = 0, star = -1, mark = 0
        while ti < t.count {
            if pi < p.count && (p[pi] == "?" || p[pi] == t[ti]) {
                ti += 1; pi += 1
            } else if pi < p.count && p[pi] == "*" {
                star = pi; mark = ti; pi += 1
            } else if star >= 0 {
                pi = star + 1; mark += 1; ti = mark
            } else {
                return false
            }
        }
        while pi < p.count && p[pi] == "*" { pi += 1 }
        return pi == p.count
    }
}
