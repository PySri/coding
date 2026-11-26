import WebKit

/// Builds a WebKit content-rule list that stops known ad / tracking hosts from loading
/// as third parties, so most tracker cookies are never set in the first place.
enum ContentBlocker {
    static let identifier = "CookieBrowserTrackerBlocker"

    static func rulesJSON() -> String {
        let rules: [[String: Any]] = TrackerRules.trackerHosts.map { host in
            let escaped = NSRegularExpression.escapedPattern(for: host)
            return [
                "trigger": [
                    "url-filter": "^https?://([^/]+\\.)?\(escaped)[:/]",
                    "load-type": ["third-party"],
                ],
                "action": ["type": "block"],
            ]
        }
        let data = try! JSONSerialization.data(withJSONObject: rules, options: [.sortedKeys])
        return String(decoding: data, as: UTF8.self)
    }

    @MainActor
    static func ruleList() async -> WKContentRuleList? {
        try? await WKContentRuleListStore.default()
            .compileContentRuleList(forIdentifier: identifier, encodedContentRuleList: rulesJSON())
    }
}
